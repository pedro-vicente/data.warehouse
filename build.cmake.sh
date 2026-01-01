#!/bin/bash
set -e

if [ ! -d "ext/asio-1.30.2" ]; then
    echo "asio not found in ext/, cloning from GitHub..."
    mkdir -p ext
    pushd ext
    git -c advice.detachedHead=false clone --branch asio-1-30-2 --depth 1 https://github.com/chriskohlhoff/asio.git asio-1.30.2
    if [ $? -ne 0 ]; then
        echo "Failed to clone asio repository"
        exit 1
    fi
    popd
else
    echo "asio already exists in ext/"
fi

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
