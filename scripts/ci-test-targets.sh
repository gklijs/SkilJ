#!/usr/bin/env bash
# shellcheck disable=SC2034 # Arrays and flags are consumed by the sourcing script.
# Sourced by ci-test.sh. Keep whole libtest binaries together: their tests
# share a database, so splitting individual tests across processes is unsafe.
ci_test_targets() {
    local group="${1:-all}"
    local test_file test_name
    test_args=()
    test_app_docs=false
    case "$group" in
        all) test_args=(--workspace) ;;
        libraries) test_args=(--workspace --exclude skilj) ;;
        business) test_args=(-p skilj --features local --test graphql_business_surfaces) ;;
        app)
            # The full workspace enables skilj/local through skilj-demo. Keep
            # that feature in this shard so local-mode tests are not lost.
            test_args=(-p skilj --features local --lib --bins --examples)
            for test_file in "$HERE"/../skilj/tests/*.rs "$HERE"/../skilj/tests/*/main.rs; do
                [[ -f "$test_file" ]] || continue
                if [[ "${test_file##*/}" == main.rs ]]; then
                    test_name="${test_file%/main.rs}"
                    test_name="${test_name##*/}"
                else
                    test_name="${test_file##*/}"
                fi
                test_name="${test_name%.rs}"
                [[ "$test_name" == graphql_business_surfaces ]] && continue
                test_args+=(--test "$test_name")
            done
            # Explicit target selection disables Cargo's implicit doctest run.
            test_app_docs=true
            ;;
        *) printf 'ci-test: unknown CI_TEST_GROUP: %s\n' "$group" >&2; return 2 ;;
    esac
}
