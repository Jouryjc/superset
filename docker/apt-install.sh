#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

# Ensure this script is run as root
if [[ $EUID -ne 0 ]]; then
  echo "This script must be run as root" >&2
  exit 1
fi

# Check for required arguments
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <package1> [<package2> ...]" >&2
  exit 1
fi

# Colors for better logging (optional)
GREEN='\033[0;32m'
RED='\033[0;31m'
RESET='\033[0m'

# 检查sources.list文件是否存在
if [ -f /etc/apt/sources.list ]; then
  # 备份原始文件
  cp /etc/apt/sources.list /etc/apt/sources.list.bak
  # 修改源
  sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list
  sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list
else
  # 文件不存在时创建
  mkdir -p /etc/apt
  echo "deb http://mirrors.aliyun.com/debian/ bookworm main" > /etc/apt/sources.list
  echo "deb http://mirrors.aliyun.com/debian-security/ bookworm-security main" >> /etc/apt/sources.list
  echo "deb http://mirrors.aliyun.com/debian/ bookworm-updates main" >> /etc/apt/sources.list
fi

# 使用无缓存更新，避免网络问题
apt-get update -o Acquire::http::No-Cache=True || {
  echo -e "${RED}APT源更新失败，尝试使用原始源${RESET}"
  # 如果备份存在则恢复
  if [ -f /etc/apt/sources.list.bak ]; then
    mv /etc/apt/sources.list.bak /etc/apt/sources.list
    apt-get update
  fi
}

# Install packages with clean-up
echo -e "${GREEN}Updating package lists...${RESET}"
apt-get update -qq

echo -e "${GREEN}Installing packages: $@${RESET}"
apt-get install -yqq --no-install-recommends "$@"

echo -e "${GREEN}Autoremoving unnecessary packages...${RESET}"
apt-get autoremove -y

echo -e "${GREEN}Cleaning up package cache and metadata...${RESET}"
apt-get clean
rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/*

echo -e "${GREEN}Installation and cleanup complete.${RESET}"
