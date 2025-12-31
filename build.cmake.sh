#!/bin/bash
set -e
path_wt="$(pwd)/install/wt"
echo "Wt at: $path_wt"

if [[ "$OSTYPE" == "msys"* ]]; then
    path_boost="$(pwd)/build/boost_1_88_0"
    echo "Boost at: $path_boost"
fi

app="data.warehouse"

mkdir -p build/$app
pushd build
pushd $app

if [[ "$OSTYPE" == "msys"* ]]; then
    cmake ../.. \
        -DWT_INCLUDE="$path_wt/include" \
        -DBOOST_INCLUDE_DIR="$path_boost/include/boost-1_88" \
        -DBOOST_LIB_DIRS="$path_boost/lib"
else
    cmake ../.. --fresh \
        -DWT_INCLUDE="$path_wt/include"
fi

cmake --build . --config Debug 

popd
popd
exit
