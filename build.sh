#!/bin/bash
# author: jinjing
# date: 2020.11.06
# des: 编译脚本
# go build -X 变量值中不能有空格

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# 编译选项
# windows
# linux
# darwin
if [ ! -n "$1" ] ;then
    echo "you need input target os { windows | linux | darwin }. -'darwin' is mac os"
    exit
else
    echo "the target os you input is $1"
fi
cross_compile=0
if [ "$2" == "-c" ] ;then
    echo "use cross compile"
    cross_compile=1
fi
echo
targetos=$1
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

repository=${PWD##*/}
# bash ./env.sh
# 重新造一遍 go mod
go env -w GOPROXY=https://goproxy.cn,direct
# bash ./shell/configure.sh

# 初始化 -v 的基本信息
appName=${PWD##*/}
# appName去掉一些特殊字符
appName=${appName//-/}
BuildTime=$(date)
BuildTime=${BuildTime// /_}
echo "BuildTime: "$BuildTime

BuildUser=$(whoami)
BuildUser=${BuildUser// /_}
echo "BuildUser: "$BuildUser

git rev-list HEAD | sort > config.git-hash
LOCALVER=`wc -l config.git-hash | awk '{print $1}'`
if [ $LOCALVER \> 1 ] ; then
    VER=`git rev-list origin/master | sort | join config.git-hash - | wc -l | awk '{print $1}'`
    if [ $VER != $LOCALVER ] ; then
        VER="$VER+$(($LOCALVER-$VER))"
    fi
    if git status | grep -q "modified:" ; then
        VER="${VER}M"
    fi
    VER="$VER $(git rev-list HEAD -n 1 | cut -c 1-7)"
    GIT_VERSION=r$VER
fi
rm -f config.git-hash
BuildVersion=$GIT_VERSION
BuildVersion=${BuildVersion// /_}
echo "BuildVersion: "$BuildVersion

check_ifconfig=`ls /sbin/ | grep ifconfig`
if [ "$check_ifconfig" = "ifconfig" ] ;then
    BuildMachine=$(/sbin/ifconfig | grep "inet" | grep -v "127.0.0.1" | grep -v "inet6" | awk '{print $2}'| tr "\n" " ")
else
    BuildMachine=$(ip addr | grep "inet" | grep -v "127.0.0.1" | grep -v "inet6" | awk '{print $2}' | awk -F'/' '{print $1}' | head -n1)
fi
BuildMachine=${BuildMachine// /_}
echo "BuildMachine: "$BuildMachine

# 清理上一次的产出并且编译
rm -f ./bin/*${appName}*

echo "CGO_ENABLED=1 GOOS=${targetos} GOARCH=amd64 go build -mod=readonly -ldflags '-X main.BuildVersion=${BuildTime}*${BuildUser}*${BuildVersion}*${BuildMachine}*${BuildVersion}' -o ./bin/${appName} ./main.go"
# CGO_ENABLED=0 GOOS=${targetos} GOARCH=amd64 go build -mod=readonly -ldflags "-linkmode ""external"" -extldflags ""-static"" -X main.BuildVersion=${BuildTime}*${BuildUser}*${BuildVersion}*${BuildMachine}*${BuildVersion}" -o ../bin/${appName} ./main.go
if [ "$cross_compile" = "1" ]; then
    echo "'brew install FiloSottile/musl-cross/musl-cross'"
    echo "you need install musl-cross first"
    CGO_ENABLED=1 GOOS=${targetos} CC=x86_64-linux-musl-gcc CCX=x86_64-linux-musl-g++ GOARCH=amd64 go build -mod=readonly -ldflags "-linkmode ""external"" -extldflags ""-static"" -X main.BuildVersion=${BuildTime}*${BuildUser}*${BuildVersion}*${BuildMachine}*${BuildVersion}" -tags musl -o ../bin/${appName} ./main.go
else
    CGO_ENABLED=0 GOOS=${targetos} GOARCH=amd64 go build -mod=readonly -ldflags "main.BuildVersion=${BuildTime}*${BuildUser}*${BuildVersion}*${BuildMachine}*${BuildVersion}" -o ./bin/${appName} ./main.go
fi

# 如果是windows的目标os，重命名一下加exe后缀
if [ ${targetos} = "windows" ]; then
    cd ./bin
    mv ${appName} ${appName}.exe
fi

echo ${appName}" build done"
