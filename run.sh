#!/usr/bin/env bash

set -eo pipefail

# disable TTY allocation
TTY=""
if [[ ! -t 1 ]]; then
  TTY="-T"
fi

DC="${DC:-run}"

function _docker_compose {
  docker compose "${DC}" ${TTY} "${@}"
}

function file_linters {
  : "Run file_linter docker container with docker compose, and run any command in arguments"
  _docker_compose file_linters "${@}"
}

function run_file_linters {
  : "Execute File Linters, which are Black, isort, and Flake8"
  printf "\n---Running Black Code Formatting---\n"
  file_linters black .

  printf "\n---Running isort Import Sorter---\n"
  file_linters isort .

  printf "\n---Running flake8 Inspector---\n"
  file_linters flake8 .
}

END_TIME_FORMAT=$"\n\Execution completed in %3lR"

time "${@:-help}"