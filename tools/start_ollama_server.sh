################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

# only works on linux
set -euo pipefail

os=$(uname -s)
echo "$os"

install_script=$(mktemp)
trap 'rm -f "$install_script"' EXIT

# The upstream installer probes for the .tar.zst asset with a silent HEAD request, and on
# any failure of that probe falls back to a .tgz that current releases no longer publish.
# A single transient network error therefore turns into a hard 404, so retry the install.
install_ollama() {
  attempts=3
  for attempt in $(seq 1 "$attempts"); do
    if curl -fsSL https://ollama.com/install.sh -o "$install_script" && sh "$install_script"; then
      return 0
    fi

    if [ "$attempt" -lt "$attempts" ]; then
      delay=$((attempt * 10))
      echo "ollama install attempt ${attempt}/${attempts} failed; retrying in ${delay}s" >&2
      sleep "$delay"
    fi
  done

  echo "ollama install failed after ${attempts} attempts" >&2
  return 1
}

install_ollama || exit 1

# llama-server selects a libggml-cpu-<microarch>.so at load time from the best match for the
# host CPU, so which kernels serve a request depends on the runner the job landed on. Record
# the CPU and the installed variants to make that attributable when a job fails.
find_ollama_lib_dir() {
  for dir in /usr/local/lib/ollama /usr/lib/ollama /opt/ollama/lib; do
    if compgen -G "${dir}/libggml-cpu-*" > /dev/null 2>&1; then
      echo "$dir"
      return 0
    fi
  done
  return 1
}

cpu_model=$(sed -n 's/^model name[[:space:]]*: *//p' /proc/cpuinfo | head -1)
has_amx=no
if grep -qm1 '^flags.*\bamx_tile\b' /proc/cpuinfo; then
  has_amx=yes
fi
echo "ollama-cpu: model='${cpu_model:-unknown}' amx_tile=${has_amx}"

lib_dir=$(find_ollama_lib_dir) || lib_dir=""
if [ -n "$lib_dir" ]; then
  echo "ollama-cpu: variants in ${lib_dir}:"
  for variant in "$lib_dir"/libggml-cpu-*; do
    echo "ollama-cpu:   $(basename "$variant")"
  done
else
  echo "ollama-cpu: no libggml-cpu-* variants found"
fi

# sapphirerapids is the only x86 variant built with AMX_TILE/AMX_INT8, and llama-server
# prefers it on AMX-capable hosts. Upstream reports a llama-server segfault on such hosts
# (ollama/ollama#17006). Removing the variant makes llama-server fall back to the next-best
# build, which has no AMX kernels.
if [ "${OLLAMA_DISABLE_AMX_VARIANT:-0}" = "1" ] && [ -n "$lib_dir" ]; then
  disabled=no
  for variant in "$lib_dir"/libggml-cpu-sapphirerapids*; do
    [ -e "$variant" ] || continue
    sudo mv "$variant" "${variant}.disabled"
    echo "ollama-cpu: disabled $(basename "$variant")"
    disabled=yes
  done

  if [ "$disabled" = yes ]; then
    # The installer runs ollama as a systemd unit; restart it so no already-loaded process
    # keeps serving from the removed variant.
    if systemctl is-active --quiet ollama; then
      sudo systemctl restart ollama
      echo "ollama-cpu: restarted ollama"
    fi
  else
    echo "ollama-cpu: no sapphirerapids variant installed; nothing to disable"
  fi
fi
