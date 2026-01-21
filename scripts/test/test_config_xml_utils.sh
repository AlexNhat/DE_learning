#!/bin/bash

function check_privilege {
    # Check if EUID is 0 (root)
    if [ "$EUID" -ne 0 ]; then
        echo "This script must be run with sudo."
        exit 1
    fi
}


function test_configure {
    declare -A test_cases=(
        ["CORE_CONF_fs_defaultFS"]="hdfs://namenode:9000 <property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>"
        ["HIVE_SITE_CONF_javax_jdo_option_ConnectionURL"]="jdbc:postgresql://hive-metastore-postgresql/metastore <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:postgresql://hive-metastore-postgresql/metastore</value></property>
"
    )

    for key in "${!test_cases[@]}"; do
        IFS=" " read -a vals <<< "${test_cases[$key]}"

        export "$key=${vals[0]}"

        result=''
        case $key in
            CORE_CONF_*)
                configure ../resources/hadoop_core_site.xml core CORE_CONF result
                ;;
            HIVE_SITE_*)
                configure ../resources/hive.xml core HIVE_SITE_CONF result
                ;;
            *)
                exit 1
                ;;
        esac

        assertEquals $result ${vals[1]}
    done
}


function main {
    set -af
    check_privilege
    . ../../docker/hadoop_ecosystem/config_xml_utils.sh
    . ../../shunit2/shunit2
    test_configure
    set +af
}

main
