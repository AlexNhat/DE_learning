#!/bin/bash

function add_property {
    local path=$1
    local name=$2
    local value=$3

    local entry="<property><name>$name</name><value>${value}</value></property>"
    local escaped_entry=$(echo $entry | sed 's/\//\\\//g')
    sed -i --follow-symlinks "/<\/configuration>/ s/.*/${escaped_entry}\n&/" $path

    if [ ! -z "${4:-}" ]; then
        eval "$4='$entry'"
    fi
}


function configure {
    local path=$1
    local module=$2
    local env_prefix=$3

    local var
    local value

    echo "Configuring $module"
    for c in $(printenv | perl -sne 'print "$1 " if m/^${env_prefix}_(.+?)=.*/' -- -env_prefix=$env_prefix); do
        name=$(echo ${c} | perl -pe 's/___/-/g; s/__/@/g; s/_/./g; s/@/_/g;')
        var="${env_prefix}_${c}"
        value=${!var}
        echo " - Setting $name=$value"

        add_property $path $name "$value" "${4:-}"
    done
}
