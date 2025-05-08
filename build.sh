# swag init
Tag=$1
Arch=$2
OS=$3
if [ -z $Arch ]
then
    Arch=amd64
fi
if [ -z $OS ]
then
    OS=linux
fi
rm -rf dist
mkdir dist
mkdir dist/conf
go mod vendor
GOWORK=off GOOS=$OS GOARCH=$Arch go build -ldflags "-s -w" -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -tags=${Tag},kerberos,$Arch -mod=vendor
mv starrocks-migrate-tool dist/
cp -r conf/config_${Tag}.conf dist/conf/
cp -f README.md dist/
rm -rf smt
rm -f smt.tar.gz
cp -r dist smt
tar -czvf smt.tar.gz smt
rm -rf smt
