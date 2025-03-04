#!/bin/bash -ie
#Note - ensure that the -e flag is set to properly set the $? status if any command fails

# Since we are using the system jruby, we need to make sure our jvm process
# uses at least 1g of memory, If we don't do this we can get OOM issues when
# installing gems. See https://github.com/elastic/logstash/issues/5179
export JRUBY_OPTS="-J-Xmx1g"
export GRADLE_OPTS="-Xmx2g -Dorg.gradle.jvmargs=-Xmx2g -Dorg.gradle.daemon=false -Dorg.gradle.logging.level=info -Dfile.encoding=UTF-8"

export SPEC_OPTS="--order rand --format documentation"
export CI=true

if [ -n "$BUILD_JAVA_HOME" ]; then
  GRADLE_OPTS="$GRADLE_OPTS -Dorg.gradle.java.home=$BUILD_JAVA_HOME"
fi

if [[ $1 = "setup" ]]; then
 echo "Setup only, no tests will be run"
 exit 0

elif [[ $1 == "split" ]]; then

    # usage: $0 split SELECTED_PARTITION [PARTITION_COUNT=2]
    selected_partition="${2:?split parameter missing}"
    partition_count="${3:-2}" # assume 2 if no PARTITION_COUNT given

    # list all specs in consistent sort order
    all_specs=($(cd qa/integration && find specs -name '*_spec.rb' | sort | uniq))

    if (( $partition_count <= 0 )) ; then
      echo "Error, partition_count(${partition_count}) must be greater than 0"; exit 1
    elif (( $partition_count > ${#all_specs[@]})); then
      echo "Error, partition_count(${partition_count}) must be less than matching specs(${#all_specs[@]})"; exit 1
    elif (( $selected_partition < 0 )) || (( $selected_partition >= $partition_count )) ; then
      echo "Error, selected_partition(${selected_partition}) must be greater 0 and less than partition_count(${partition_count})"; exit 1
    fi

    # index and sort the specs by their estimated cost, which is in a comment including SPLIT_ESTIMATE: INTEGER
    # the result is an array of filename-tab-cost entries with default values and the most expensive entries first.
    indexed=()
    default_cost=20
    cost_index="$(cat qa/integration/timings.tsv || true)"
    for index in "${!all_specs[@]}"; do
      file="${all_specs[$index]}"
      cost="$(awk -F':' '{ if (match($1,"SPLIT_ESTIMATE")) { print sprintf("%d", $2); exit } }' "qa/integration/${file}")"
      indexed+=("${file}"$'\t'"${cost:-${default_cost}}")
    done
    IFS=$'\n' indexed=($(printf "%s\n" "${indexed[@]}" | sort --numeric-sort -k2rn -k1)); unset IFS

    # iterate over them, assigning each to the least-allocated partition
    selected_specs=()
    partition_costs=()
    for index in "${!indexed[@]}"; do
      # split entry into columns
      IFS=$'\t' read -r file cost <<< "${indexed[$index]}"

      # find the partition with the current lowest cost
      partition=0
      for i in $(seq 1 $(( partition_count - 1 ))); do
        if (( "${partition_costs[$i]:-0}" < "${partition_costs[${partition}]:-0}" )); then
          partition=$i
        fi
      done

      # allocate the cost of this file to its partition
      partition_costs[$partition]=$(( partition_costs[$partition] + $cost ))

      # add the file to the selected_specs IFF it is our selected partition
      if (( $partition == $selected_partition )); then
        selected_specs+=("${file}")
        >&2 echo "[PICK] ${file} (estimate: ${cost})"
      fi
    done

    echo "Running integration specs split[${selected_partition}] of ${partition_count} (estimate: ${partition_costs[$selected_partition]}): ${selected_specs[*]}"
    ./gradlew runIntegrationTests -PrubyIntegrationSpecs="${selected_specs[*]}" --console=plain

elif [[ !  -z  $@  ]]; then
    echo "Running integration tests 'rspec $@'"
    ./gradlew runIntegrationTests -PrubyIntegrationSpecs="$@" --console=plain

else
    echo "Running all integration tests"
    ./gradlew runIntegrationTests --console=plain
fi
