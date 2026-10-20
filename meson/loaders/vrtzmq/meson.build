deps = []
vrtzmq_supported = (jst_is_linux or jst_is_macos) and not jst_is_static

if vrtzmq_supported
    deps += [
        dependency('libzmq', required: false),
        meson.get_compiler('c').find_library('vrt',
                                             has_headers: ['vrt/vrt_read.h'],
                                             required: false),
    ]
endif

all_deps_found = vrtzmq_supported
foreach x_dep : deps
    all_deps_found = all_deps_found and x_dep.found()
endforeach

if all_deps_found
    cfg_lst.set('JETSTREAM_LOADER_VRTZMQ_AVAILABLE', true)
    dep_lst += deps
endif

ldr_lst += {'VRTZMQ': all_deps_found}
