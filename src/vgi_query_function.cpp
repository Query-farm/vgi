// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_query_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_query_rpc.hpp"
#include "vgi_table_function_impl.hpp"

#include "nanoarrow/nanoarrow.h"

#include <chrono>
#include <atomic>
#include <set>

namespace duckdb {
namespace vgi {

// Copies of a bound plan share the preparation. The weak context avoids keeping
// a connection alive for EXPLAIN/PREPARE plans which never initialize a reader.
// Release is attempted once; a failed transport is left to the provider's TTL.
class VgiQueryPreparationLease {
public:
	VgiQueryPreparationLease(ClientContext &context, CatalogRpcContext rpc_context,
	                         std::vector<uint8_t> token, int64_t expires_at)
	    : context(context.shared_from_this()), rpc_context(std::move(rpc_context)), token(std::move(token)),
	      expires_at(expires_at) {
	}

	~VgiQueryPreparationLease() {
		auto live = context.lock();
		if (live) {
			ReleaseNoThrow(*live);
		}
	}

	void ClaimExecution() {
		auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
		               std::chrono::system_clock::now().time_since_epoch()).count();
		if (released.load() || now >= expires_at || initialized.exchange(true)) {
			throw IOException("vgi_query preparation has expired or was already consumed; execute a fresh query");
		}
	}

	void Release(ClientContext &live) {
		if (released.exchange(true)) {
			return;
		}
		InvokeCatalogQueryRelease(rpc_context, token, live);
	}

	void ReleaseNoThrow(ClientContext &live) noexcept {
		try {
			Release(live);
		} catch (...) {
		}
	}

private:
	weak_ptr<ClientContext> context;
	CatalogRpcContext rpc_context;
	std::vector<uint8_t> token;
	int64_t expires_at;
	std::atomic<bool> released {false};
	std::atomic<bool> initialized {false};
};

namespace {

constexpr int64_t MAX_PARAMETER_BYTES = 8 * 1024 * 1024;
constexpr idx_t MAX_PARAMETERS = 1024;

// Arrow ValidateFull checks buffers, but not field nullability. Walk only
// visible children: a required child beneath a NULL parent is not a value.
void ValidateRequiredValues(const ArrowSchema &field, const VgiColumn &column, int64_t begin, int64_t end,
                            std::optional<bool> nullable_override = std::nullopt) {
	const bool nullable = nullable_override.value_or((field.flags & ARROW_FLAG_NULLABLE) != 0);
	if (!column.is_dictionary() && field.n_children == 0 && (nullable || column.null_count() == 0)) return;
	for (int64_t row = begin; row < end; ++row) {
		if (column.IsNull(row)) {
			if (!nullable) throw IOException("vgi_query result contains NULL in required field '%s'", field.name ? field.name : "");
			continue;
		}
		if (column.is_dictionary()) {
			auto index = column.GetInt(row);
			ValidateRequiredValues(*field.dictionary, column.dictionary(), index, index + 1, nullable);
			continue;
		}
		switch (column.storage_type()) {
		case NANOARROW_TYPE_STRUCT:
			for (int64_t child = 0; child < field.n_children; ++child) {
				ValidateRequiredValues(*field.children[child], column.child(child), row, row + 1);
			}
			break;
		case NANOARROW_TYPE_LIST:
		case NANOARROW_TYPE_MAP:
		case NANOARROW_TYPE_LARGE_LIST:
		case NANOARROW_TYPE_FIXED_SIZE_LIST: {
			int64_t child_begin, child_end;
			column.ListRange(row, child_begin, child_end);
			ValidateRequiredValues(*field.children[0], column.child(0), child_begin, child_end);
			break;
		}
		case NANOARROW_TYPE_SPARSE_UNION:
		case NANOARROW_TYPE_DENSE_UNION: {
			auto index = row + column.shift();
			auto child = ArrowArrayViewUnionChildIndex(&column.view(), index);
			auto offset = ArrowArrayViewUnionChildOffset(&column.view(), index);
			ValidateRequiredValues(*field.children[child], column.child(child), offset, offset + 1);
			break;
		}
		default:
			break;
		}
	}
}

void CancelNoThrow(IFunctionConnection *connection, ClientContext &context) noexcept {
	if (!connection || connection->IsFinished()) {
		return;
	}
	try {
		connection->CancelStream(connection->GetLastStateToken(), context);
	} catch (...) {
	}
}

struct VgiQueryGlobalState : VgiTableFunctionGlobalState {
	explicit VgiQueryGlobalState(ClientContext &context) : context(context.shared_from_this()) {
	}
	~VgiQueryGlobalState() override {
		// LIMIT 0, initialization failure, or interruption may prevent a local
		// reader from claiming the independently owned execution.
		auto live = context.lock();
		if (live) {
			CancelNoThrow(primary_connection.get(), *live);
		}
	}
	weak_ptr<ClientContext> context;
};

FunctionConnectionParams QueryConnectionParams(const VgiTableFunctionBindData &data, const char *phase) {
	FunctionConnectionParams params;
	params.attach_params = data.attach_params;
	params.attach_opaque_data = data.attach_opaque_data;
	params.transaction_opaque_data = data.transaction_opaque_data;
	params.function_name = data.function_name;
	params.arguments = data.arguments;
	params.function_type = "TABLE";
	params.phase = phase;
	return params;
}

VgiBatchPtr BuildQueryParameters(ClientContext &context, TableFunctionBindInput &input) {
	std::string mode = "named";
	auto mode_it = input.named_parameters.find("parameter_mode");
	if (mode_it != input.named_parameters.end()) {
		if (mode_it->second.IsNull()) {
			throw BinderException("vgi_query parameter_mode must not be NULL");
		}
		mode = mode_it->second.GetValue<std::string>();
	}
	if (mode != "named" && mode != "positional") {
		throw BinderException("vgi_query parameter_mode must be 'named' or 'positional'");
	}
	vector<Value> values;
	vector<string> names;
	auto parameters_it = input.named_parameters.find("parameters");
	if (parameters_it != input.named_parameters.end()) {
		auto &parameters = parameters_it->second;
		const auto type_id = parameters.type().id();
		if (parameters.IsNull() ||
		    (type_id != LogicalTypeId::STRUCT && !(mode == "positional" && type_id == LogicalTypeId::TUPLE))) {
			throw BinderException("vgi_query parameters must be a non-NULL STRUCT, or row(...) in positional mode");
		}
		values = StructValue::GetChildren(parameters);
		if (values.size() > MAX_PARAMETERS) {
			throw BinderException("vgi_query supports at most 1024 parameters");
		}
		std::set<string> seen;
		for (idx_t i = 0; i < values.size(); i++) {
			if (values[i].type().id() == LogicalTypeId::SQLNULL) {
				throw BinderException("vgi_query NULL parameters require an explicit SQL type cast");
			}
			auto name = mode == "positional" ? std::to_string(i + 1) : StructType::GetChildName(parameters.type(), i).GetIdentifierName();
			if (mode == "named") {
				bool numeric = !name.empty() && name.find_first_not_of("0123456789") == string::npos;
				if (name.empty() || numeric || string("$:@?").find(name[0]) != string::npos ||
				    !seen.insert(name).second) {
					throw BinderException("vgi_query named parameters need unique nonempty names without placeholder prefixes");
				}
			}
			names.push_back(std::move(name));
		}
	}
	auto arguments = BuildArgumentsFromValues(context, values);
	auto argument_column = arguments.batch->column(0);
	ArrowSchemaWrapper parameter_schema;
	if (ArrowSchemaDeepCopy(&argument_column.schema(), &parameter_schema.arrow_schema) != NANOARROW_OK) {
		throw InternalException("VGI failed to copy parameter schema");
	}
	ArrowSchemaSetName(&parameter_schema.arrow_schema, "");
	parameter_schema.arrow_schema.flags = 0;
	for (idx_t i = 0; i < values.size(); ++i) {
		if (ArrowSchemaSetName(parameter_schema.arrow_schema.children[i], names[i].c_str()) != NANOARROW_OK) {
			throw InternalException("VGI failed to name parameter field");
		}
	}
	const auto metadata = VgiMetadata({{"vgi.query.parameters.v1", mode}}).ToC();
	if (ArrowSchemaSetMetadata(&parameter_schema.arrow_schema, metadata.data()) != NANOARROW_OK) {
		throw InternalException("VGI failed to set parameter metadata");
	}
	VgiBatchBuilder builder(VgiSchema::Adopt(&parameter_schema.arrow_schema));
	for (idx_t i = 0; i < values.size(); ++i) AppendElement(builder.column(i), argument_column.child(i), 0);
	builder.FinishRow();
	auto batch = builder.Finish();
	ValidateBatch(*batch, VgiValidation::FULL);
	return batch;
}

unique_ptr<FunctionData> VgiQueryBind(ClientContext &context, TableFunctionBindInput &input,
	                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (!IsVgiQueryEnabled(context)) {
		throw BinderException("vgi_query is disabled; SET vgi_query_enabled=true before attaching the catalog");
	}
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("vgi_query catalog and SQL must not be NULL");
	}
	auto catalog_name = input.inputs[0].GetValue<string>();
	auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
	if (catalog.GetCatalogType() != "vgi") {
		throw BinderException("vgi_query catalog '%s' is not a VGI catalog", catalog_name);
	}
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	auto attach = vgi_catalog.attach_result();
	if (!attach || !attach->query_pushdown) {
		throw BinderException("vgi_query explicit query execution was not negotiated for catalog '%s'", catalog_name);
	}
	auto parameters = BuildQueryParameters(context, input);
	auto parameter_bytes = SerializeRecordBatch(parameters);
	if (parameter_bytes->size() > MAX_PARAMETER_BYTES) {
		throw BinderException("vgi_query parameter batch exceeds the 8 MiB protocol limit");
	}
	auto data = make_uniq<VgiTableFunctionBindData>();
	data->explicit_query = true;
	data->fixed_order = true;
	data->attach_params = vgi_catalog.attach_parameters();
	data->attach_opaque_data = attach->attach_opaque_data;
	data->transaction_opaque_data = VgiTransaction::Get(context, catalog).GetTransactionOpaqueData();
	data->function_name = "__vgi_catalog_query";
	CatalogRpcContext rpc_context {data->attach_params, data->attach_opaque_data, data->transaction_opaque_data};
	auto prepared = InvokeCatalogQueryPrepare(rpc_context, *attach->query_pushdown,
	                                        input.inputs[1].GetValue<string>(), parameters->schema(), context,
	                                        "query_order");
	if (!prepared.accepted) {
		throw BinderException("vgi_query preparation was declined: %s", prepared.reason_code);
	}
	data->query_preparation = std::make_shared<VgiQueryPreparationLease>(
	    context, rpc_context, prepared.prepared_query_token, prepared.expires_at_epoch_ms);
	try {
		vector<Value> args;
		args.push_back(Value::BLOB(prepared.prepared_query_token.data(),
		                           prepared.prepared_query_token.size()));
		args.push_back(Value::BLOB(parameter_bytes->data(), parameter_bytes->size()));
		data->arguments = BuildArgumentsFromValues(context, args);
		// Direct bind: never replay a query RPC after a transport error.
		auto acquired = AcquireConnectionForInit(context, QueryConnectionParams(*data, "query_bind"));
		data->bind_result = acquired.connection->PerformBindRpc();
		if (!data->bind_result.output_schema ||
		    !data->bind_result.output_schema->Equals(*prepared.output_schema, true)) {
			throw IOException("vgi_query bind schema differs from the prepared schema (including metadata)");
		}
		ArrowSchemaToDuckDBTypes(context, data->bind_result.output_schema, data->c_schema,
		                         data->arrow_table, return_types, names);
		data->all_column_names = IdentifiersToStrings(names);
		data->all_column_types = return_types;
	} catch (...) {
		data->query_preparation->ReleaseNoThrow(context);
		throw;
	}
	return std::move(data);
}

unique_ptr<GlobalTableFunctionState> VgiQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<VgiTableFunctionBindData>();
	if (!IsVgiQueryEnabled(context)) {
		throw InvalidInputException("vgi_query was disabled before execution");
	}
	data.query_preparation->ClaimExecution();
	auto state = make_uniq<VgiQueryGlobalState>(context);
	state->fixed_order = true;
	state->max_processes = 1;
	state->client_context_for_explain = &context;
	try {
		auto acquired = AcquireConnectionForInit(context, QueryConnectionParams(data, "query_init"));
		state->primary_connection = std::move(acquired.connection);
		// No filters, projections, splits, hints, caching, secondary readers, or
		// retry: a failed init can already have started remote execution.
		auto result = state->primary_connection->PerformInit(data.bind_result);
		if (result.max_workers != 1) {
			throw IOException("vgi_query executor must return exactly one reader");
		}
		state->global_execution_id = std::move(result.execution_id);
		state->init_opaque_data = std::move(result.opaque_data);
		state->init_applied.store(true, std::memory_order_release);
		// The execution is independently owned only after init has succeeded.
		data.query_preparation->Release(context);
	} catch (...) {
		data.query_preparation->ReleaseNoThrow(context);
		throw;
	}
	return std::move(state);
}

} // namespace

void ValidateVgiQueryResultBatch(const VgiBatchPtr &batch,
                                 const VgiSchemaPtr &expected) {
	if (!batch) {
		return;
	}
	if (!batch->schema()->Equals(*expected, true)) {
		throw IOException("vgi_query result schema differs from the prepared schema (including metadata)");
	}
	ValidateBatch(*batch, VgiValidation::FULL);
	for (int column = 0; column < batch->num_columns(); column++) {
		ValidateRequiredValues(expected->field(column), batch->column(column), 0, batch->num_rows());
	}
}

void RegisterVgiQueryFunction(ExtensionLoader &loader) {
	TableFunction function("vgi_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, VgiTableFunctionScan,
	                       VgiQueryBind, VgiQueryInitGlobal, VgiTableFunctionInitLocal);
	vgi::AddVgiFunctionOption(function, Identifier("parameters"), LogicalType::ANY);
	vgi::AddVgiFunctionOption(function, Identifier("parameter_mode"), LogicalType::VARCHAR);
	function.projection_pushdown = false;
	function.filter_pushdown = false;
	function.sampling_pushdown = false;
	function.order_preservation_type = OrderPreservationType::FIXED_ORDER;
	CreateTableFunctionInfo info(function);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Execute explicitly selected remote read-only SQL through an attached VGI catalog. Parameters are typed "
	    "STRUCT values for named parameters (default), or row(...) with parameter_mode := positional. "
	    "Requires vgi_query_enabled before ATTACH.",
	    {"catalog", "sql"}, {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    {"SELECT * FROM vgi_query('remote', 'SELECT $value', parameters := {'value': 7::INTEGER});"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
