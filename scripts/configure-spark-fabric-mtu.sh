#!/usr/bin/env bash
set -euo pipefail
# Only the two existing direct Spark profiles. No IP, route or Wi-Fi changes.
mode="${1:---check}"
[[ "$mode" == --check || "$mode" == --apply || "$mode" == --rollback ]] || {
  echo 'Usage: configure-spark-fabric-mtu.sh [--check|--apply|--rollback]' >&2; exit 2;
}
case "$(hostname)" in
  spark-66f1) suffix=1 ;;
  spark-e8f1) suffix=2 ;;
  *) echo 'Unknown host; edit the inventory and review fabric setup first.' >&2; exit 1 ;;
esac
profiles=(spark-link-20 spark-link-21)
interfaces=(enp1s0f0np0 enP2p1s0f0np0)
for i in 0 1; do
  [[ "$(nmcli -g connection.interface-name connection show "${profiles[$i]}")" == "${interfaces[$i]}" ]] || {
    echo 'Profile/interface mismatch; no changes made.' >&2; exit 1;
  }
  expected="10.10.$((20+i)).$suffix/30"
  [[ "$(nmcli -g ipv4.addresses connection show "${profiles[$i]}")" == "$expected" ]] || {
    echo 'Profile/address mismatch; no changes made.' >&2; exit 1;
  }
done
if [[ "$mode" == --check ]]; then
  for interface in "${interfaces[@]}"; do
    printf '%s MTU=' "$interface"
    cat "/sys/class/net/$interface/mtu"
  done
  exit 0
fi
(( EUID == 0 )) || { echo 'Run this operation with sudo.' >&2; exit 1; }
if [[ -n "$(nvidia-smi --query-compute-apps=pid --format=csv,noheader)" ]]; then
  echo 'GPU work is active. Stop the relevant workloads before changing fabric MTU.' >&2
  exit 1
fi
backup=/var/lib/local-llm-stack/fabric-mtu-backup.tsv
mkdir -p "$(dirname "$backup")"
if [[ "$mode" == --apply ]]; then
  if [[ ! -f "$backup" ]]; then
    temporary="$(mktemp "$(dirname "$backup")/.mtu.XXXXXX")"
    for i in 0 1; do
      configured="$(nmcli -g 802-3-ethernet.mtu connection show "${profiles[$i]}")"
      [[ "$configured" != auto ]] || configured=0
      printf '%s\t%s\t%s\t%s\n' "${profiles[$i]}" "${interfaces[$i]}" \
        "$configured" \
        "$(cat "/sys/class/net/${interfaces[$i]}/mtu")" >> "$temporary"
    done
    chmod 600 "$temporary"
    mv "$temporary" "$backup"
  fi
  for i in 0 1; do
    nmcli connection modify "${profiles[$i]}" 802-3-ethernet.mtu 9000
    ip link set dev "${interfaces[$i]}" mtu 9000
  done
  echo 'Fabric MTU set to 9000. Apply on both Sparks before starting distributed work.'
else
  [[ -f "$backup" ]] || { echo 'No recorded MTU backup.' >&2; exit 1; }
  while IFS=$'\t' read -r profile interface configured active; do
    [[ "$profile" == spark-link-20 || "$profile" == spark-link-21 ]] || exit 1
    [[ "$interface" == enp1s0f0np0 || "$interface" == enP2p1s0f0np0 ]] || exit 1
    [[ "$configured" =~ ^[0-9]+$ && "$active" =~ ^[0-9]+$ ]] || exit 1
    nmcli connection modify "$profile" 802-3-ethernet.mtu "$configured"
    ip link set dev "$interface" mtu "$active"
  done < "$backup"
  echo 'Original fabric MTUs restored.'
fi
bash "$0" --check
