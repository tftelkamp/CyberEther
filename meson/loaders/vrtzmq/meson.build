deps = []

if (jst_is_linux or jst_is_macos) and not jst_is_static
    deps += [
        dependency('cppzmq', 'CPPZMQ', required: true),
        #dependency('libvrt', 'LIBVRT', required: true),
        meson.get_compiler('c').find_library('vrt', required: true),
    ]
endif

all_deps_found = true

foreach x_dep : deps
    all_deps_found = all_deps_found and x_dep.found()
endforeach

if all_deps_found
    cfg_lst.set('JETSTREAM_LOADER_VRTZMQ_AVAILABLE', true)
    dep_lst += deps
endif

cfg_lst.set('JETSTREAM_LOADER_VRTZMQ_AVAILABLE', true)

ldr_lst += {'VRTZMQ': all_deps_found}
