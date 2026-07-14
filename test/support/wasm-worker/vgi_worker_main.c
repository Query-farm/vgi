// Trivial entry for the VGI browser worker module. The module is driven by its
// exported serve entry (vgi_rust_serve_table_sab_slot), called from the boot JS
// dispatcher after the channel buffer is delivered — main does nothing and the
// runtime stays alive (EXIT_RUNTIME=0) to service the exported calls.
int main(void) { return 0; }
