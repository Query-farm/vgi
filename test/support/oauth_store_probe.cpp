// © Copyright 2026 Query Farm LLC - https://query.farm
// Child process for the native Windows OAuth store tests.

#include "vgi_oauth.hpp"
#include "vgi_oauth_store.hpp"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace duckdb::vgi;

static void Check(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static OAuthRefreshContext TestBinding() {
	OAuthRefreshContext ctx;
	ctx.token_endpoint = "https://issuer.example.invalid/token";
	ctx.issuer = "https://issuer.example.invalid";
	ctx.resource = "https://resource.example.invalid/vgi";
	ctx.client_id = "test-client";
	ctx.scope = "openid offline_access";
	return ctx;
}

int main(int argc, char **argv) {
	try {
		Check(argc >= 4, "Usage: oauth_store_probe operation key mode [value]");
		const std::string operation = argv[1], key = argv[2], mode = argv[3];
		const auto binding = TestBinding();
		auto lease = AcquireOAuthCredentialLease(key, mode);
		// All persistent test cases have an available backend. In particular,
		// auto must not silently fall back after encountering an abandoned mutex.
		Check(bool(lease) == (mode == "persistent" || mode == "auto"), "Unexpected credential lease availability");
		if (operation == "store") {
			Check(argc == 5, "Missing synthetic token");
			StoreOAuthRefreshToken(key, binding, mode, argv[4]);
		} else if (operation == "read") {
			Check(argc == 5, "Missing expected token");
			std::string token;
			Check(LoadOAuthRefreshToken(key, binding, mode, token), "Credential missing");
			Check(token == argv[4], "Unexpected stored token");
		} else if (operation == "absent") {
			std::string token;
			Check(!LoadOAuthRefreshToken(key, binding, mode, token), "Credential unexpectedly present");
		} else if (operation == "delete") {
			DeleteOAuthRefreshToken(key, mode);
		} else if (operation == "increment") {
			std::string token;
			Check(LoadOAuthRefreshToken(key, binding, mode, token), "Counter missing");
			std::this_thread::sleep_for(std::chrono::milliseconds(75));
			StoreOAuthRefreshToken(key, binding, mode, std::to_string(std::stoi(token) + 1));
		} else if (operation == "hold") {
			std::cout << "LOCKED" << std::endl;
			Sleep(INFINITE);
		} else {
			throw std::runtime_error("Unknown operation");
		}
		std::cout << "OK " << operation << '\n';
		return 0;
	} catch (const std::exception &e) {
		std::cerr << e.what() << '\n';
		return 1;
	}
}
