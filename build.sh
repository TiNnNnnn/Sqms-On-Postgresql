#!/bin/bash

RESET="\033[0m"
BOLD="\033[1m"
RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"

# Default value for debug flag
DEBUG=false

# Check if debug flag is passed as an argument
while getopts "d" opt; do
  case $opt in
    d)
      DEBUG=true
      ;;
    *)
      ;;
  esac
done

echo -e "${BLUE}==========================================${RESET}"
echo -e "${GREEN}${BOLD}PostgreSQL Version:${RESET} $(pg_config --version)"
echo -e "${BLUE}==========================================${RESET}"

# Clean previous builds
echo -e "${YELLOW}${BOLD}Cleaning previous builds...${RESET}"
make clean
echo -e "${YELLOW}Clean complete.${RESET}"

# Building the extension with debug option if enabled
if [ "$DEBUG" = true ]; then
  echo -e "${YELLOW}${BOLD}Building the extension in debug mode...${RESET}"
  make -j64 enable_debug=1
else
  echo -e "${YELLOW}${BOLD}Building the extension...${RESET}"
  make -j64
fi

if [ "$DEBUG" = true ]; then
  echo -e "${YELLOW}Build complete (Debug mode enabled).${RESET}"
else
  echo -e "${YELLOW}Build complete.${RESET}"
fi

# Installing the extension
echo -e "${YELLOW}${BOLD}Installing the extension...${RESET}"
sudo env "PATH=$PATH" make install
echo -e "${GREEN}Installation complete.${RESET}"

echo -e "${BLUE}==========================================${RESET}"
echo -e "${GREEN}${BOLD}Setup finished successfully!${RESET}"
echo -e "${BLUE}==========================================${RESET}"
