# Spark connections: console, Internet, management Ethernet and trusted SSH

Use this before installing a controller, starting a model or diagnosing distributed
serving. It is usable from a local console without an LLM, Docker or working
inference. Continue with the [single-node](SPARK_SINGLE_NODE_RUNBOOK.md) or
[multi-node](SPARK_MULTI_NODE_RUNBOOK.md) runbook only after the handoff checklist.
For lost state, offline materials and restoration, use the
[emergency recovery runbook](SPARK_RECOVERY_RUNBOOK.md). [CLUSTER.md](CLUSTER.md)
is the common entrypoint.

**This is a procedure, not an execution receipt or approval.** The recorded rollout
is paused before Spark GPU mutation and fault testing. No commands in this document
were executed as part of writing it. Do not reboot, unplug live links, restart
workers, run inference or enable automatic recovery to test connectivity.

Recorded Mac-only installation evidence at revision
`65b05d5503e6ffefd2f07d07ebe86333917d5086`: all four launchd roles ran, the
authenticated gateway reported `accepting:false`/`active_requests:0`, and recovery
reported `enabled:false`/epoch `0`. All four roles are now confirmed stopped;
their plists/config/state remain. Spark workers were untouched. These observations
are not hardware/fault qualification and do not authorize further disruption.

## 1. Where commands run and when to stop

- **Spark console** means a keyboard/display physically attached to the identified
  Spark, logged in as its intended ordinary Linux user. Complete the vendor's local
  first-boot/account setup if necessary. Do not reinstall an existing machine to
  regain SSH. Without a usable display/account/admin credential, obtain the site's
  console/recovery access; network commands cannot bypass this prerequisite.
- **Mac** means a local Terminal on the controller. Shell examples use Bash; enter
  `bash` first if the login shell differs. Variables are local to each shell: values
  set on the Mac do not exist on the Spark.
- **Inspection** reads addresses, state or logs; probes send small network packets
  but neither inference nor GPU work. Commands containing `sudo` may prompt locally.
- **ADMIN / NETWORK-DISRUPTIVE** sections change persistent networking, keys or the
  SSH service. Read their rollback first. Require explicit operator approval,
  physical console access, an independent working path and a second login session
  wherever one already exists. Never change the interface carrying your only SSH
  session. On a new host with no remote session yet, the working local console is
  the recovery path; do not leave it until remote access is proved.
- **GPU-DISRUPTIVE / INFERENCE:** none is needed here. Collective-network changes
  and serving acceptance belong to the linked playbooks and their separate gates.

There is no unattended sudo on the recorded Mac or Sparks. An administrator must
provision network/service changes interactively. Docker root access is not an
acceptable workaround. Do not weaken SSH host checking, disable the firewall,
flush routes, enable Internet Sharing, or replace existing network configuration
wholesale. A host-key mismatch, duplicate address, unknown cable or unknown network
owner is a **stop condition**, not a reason to force the command.

## 2. Identify the machine and label actual cables

At each **Spark console — inspection**:

```bash
hostnamectl
id
uname -m
ip -br link
ip -br -4 address
ip -4 route show
ip -6 route show
```

Expected: the intended hostname/account, `aarch64` on a Spark, and individually
identifiable NIC names/MAC addresses. Record chassis serial/asset label, hostname,
NIC name, MAC address, physical socket, cable label and far-end socket. Hostname
alone is not host identity; independently record the SSH host key in section 7.

On the **Mac — inspection**:

```bash
networksetup -listallhardwareports
networksetup -listnetworkserviceorder
networksetup -listallnetworkservices
ifconfig
netstat -rn -f inet
scutil --dns
```

Match device names to hardware ports and network service names. `en0` is a recorded
fact for this Mac, **not** a universal Ethernet device name. A USB adapter may have
a different name after replacement. Follow each cable visually; record the switch
port if there is a switch. Check link LEDs and `LOWER_UP`/`status: active` together.
If ambiguity remains, an approved maintenance-window cable disconnect/reconnect
can identify a socket while observing `ip monitor link` at the console; reconnect
the *same labelled cable to the same socket* to roll back. Never do this on a live
collective or the only management path. A passive visual check is preferable.

### Recorded address plan versus a configurable installation

| Role | Recorded device/address | Physical/path meaning |
| --- | --- | --- |
| Mac management Ethernet | `en0`, `10.10.10.1/24` | Dedicated management LAN; no router or DNS setting |
| Spark 66f1 management | `enP7s7`, `10.10.10.2/24` | Alias `spark-66f1-wired`; Linux user `statsparrot` |
| Spark e8f1 management | `enP7s7`, `10.10.10.3/24` | Alias `spark-e8f1-wired`; Linux user `statsparrot` |
| Spark Internet | `wlP9s9`, DHCP | Wi-Fi, recorded default gateway `192.168.1.1`; last observed addresses `.31` and `.18` are leases, not static assignments |
| First collective interface | Both: `enp1s0f0np0`; 66f1 `10.10.20.1`, e8f1 `10.10.20.2` | Direct Spark-to-Spark fabric; expected prefix `/30` |
| Second collective interface | Both: `enP2p1s0f0np0`; 66f1 `10.10.21.1`, e8f1 `10.10.21.2` | Second logical rail; expected prefix `/30` |

Both Sparks and the Mac need a real shared management Layer-2 path to use the
recorded `/24` simultaneously (for example a dedicated Ethernet switch). One Mac
socket cannot independently cable directly to two Sparks. A one-Spark installation
may use a direct Mac-to-Spark Ethernet cable. Do not assume the switch, adapter,
port arrangement or chassis socket position from this table: map them on site.
The two fabric interfaces are recorded as two logical PCIe paths on **one physical
200-GbE cable**, not two independently redundant cables. See the
[multi-node physical fabric procedure](SPARK_MULTI_NODE_RUNBOOK.md).

`cluster/inventory.json` contains fabric addresses but **no masks**.
`scripts/configure-spark-fabric-mtu.sh` checks the existing profiles
`spark-link-20`/`spark-link-21` against `/30`, matching the recorded before-
observation on both Sparks. Inspect the current actual profiles at the console
before any fabric work; do not change masks during management recovery. Neither
`/30` has room for a third host.

For a different installation, first choose a private management subnet that does
not overlap Wi-Fi, VPN, container, fabric or other local routes. Reserve unique
addresses with the site administrator, write the table, and substitute the verified
NIC names, users and addresses below. A failed ping is **not proof an address is
unused**; inspect the authoritative address allocation and the existing devices.
Do not renumber an operating site just to match these examples.

Fabric carries collectives, rendezvous and some explicitly authorized peer copies;
it must not supply the Internet default route. Management must survive loss of the
fabric. A serving listener may bind a fabric address, a management address or
loopback; trusted SSH does not prove that listener is reachable. In the recorded
setup, the Mac route to legacy `10.10.20.2:8125` follows the ordinary default route,
so that serving path remains **unproven**. This runbook does not add a host route,
turn a Spark into a router, or change a serving binding to hide that gate.

## 3. Establish the existing network owner and save a before-state

At each **Spark console — inspection**:

```bash
systemctl is-active NetworkManager
systemctl is-active systemd-networkd
command -v nmcli
command -v netplan
networkctl list
```

If installed, inspect:

```bash
nmcli general status
nmcli device status
nmcli connection show
sudo netplan get
```

Inactive services or absent commands are observations, not instructions to install
or enable them. NetworkManager and networkd can both run for different interfaces;
identify the owner of the **specific** Wi-Fi/management interface. Read the existing
files under `/etc/netplan`, `/etc/NetworkManager/system-connections`, and
`/etc/systemd/network` as appropriate. Network configuration may contain Wi-Fi
secrets: inspect locally, never paste into a public ticket or use `--show-secrets`
in a shared transcript.

**Use sections 4 and 5 only if NetworkManager owns the relevant interface and its
profiles are persistently managed there.** If netplan/cloud-init/provisioning
supplies the profile, edit the existing authoritative configuration through that
owner instead. Do not create a second direct NM profile alongside generated
netplan configuration, change renderers, mark an unmanaged NIC managed, or start a
competing daemon. The portable instruction in that case is to give the owning
administrator the intended address/no-gateway policy and require a reviewed
owner-native apply and rollback. For an existing netplan owner, `sudo netplan try`
provides a timed confirmation window, but inspect its locally installed help and
rollback limitations first; maintain console access and back up its source files.
No universal YAML replacement is safe without the site's current configuration.

Create a separate private evidence directory on **each host**, including the Mac:

```bash
umask 077
EVIDENCE=$(mktemp -d "$HOME/spark-connectivity.XXXXXXXX")
printf '%s\n' "$EVIDENCE"
```

At the **Spark console**, record non-secret state:

```bash
ip -br link > "$EVIDENCE/link.before.txt"
ip -br address > "$EVIDENCE/address.before.txt"
ip route show table all > "$EVIDENCE/routes.before.txt"
nmcli connection show > "$EVIDENCE/nm-connections.before.txt"
nmcli device show > "$EVIDENCE/nm-devices.before.txt"
```

Run the NM lines only for an NM-owned host. Record profile UUIDs and autoconnect
settings, not just names. Securely back up the owning configuration files with
permissions intact before changing an existing profile; their contents may contain
credentials. Do not put those backups in Git. Keep the before-state accessible
from the console when the network is down.

## 4. Get Internet on the Spark before relying on SSH

### 4.1 Diagnose each layer — Spark console, inspection

Bind the verified device and an approved external diagnostic address; the example
uses a public DNS service address only as a routing/ICMP target:

```bash
WIFI=wlP9s9
INTERNET_PROBE=1.1.1.1
ip -br -4 address show dev "$WIFI"
ip -4 route show default
ip -4 route get "$INTERNET_PROBE"
resolvectl status
getent ahostsv4 pypi.org
```

If `resolvectl` is not installed/active, inspect `/etc/resolv.conf` and the owner's
DNS settings. Do not overwrite a managed resolver file.

1. **Radio/link:** `nmcli radio wifi`, `nmcli device status`, and
   `nmcli device wifi list ifname "$WIFI"` should show an enabled radio and the
   expected SSID. Missing device means hardware/driver/owner diagnosis first.
   Check `rfkill list` if installed; hardware blocks require the physical control.
2. **Address:** DHCP should assign an address in the actual router's subnet, not
   `169.254.x.x`. A visible SSID without a lease is not Internet connectivity.
3. **Route:** `ip route get` must choose the Internet interface and router, not
   either collective rail or the dedicated management link. Multiple defaults
   require owner/metric investigation; do not flush all routes.
4. **Gateway/IP:** after reading the real default route, bind `GATEWAY` to that
   address, then `ping -c 3 "$GATEWAY"` and `ping -c 3 "$INTERNET_PROBE"`.
   ICMP may be filtered, so its failure alone is inconclusive.
5. **DNS/application transport:** `getent` should return addresses, then
   `curl --head --connect-timeout 10 --max-time 20 https://pypi.org/simple/`
   should complete TLS and return an HTTP response. This is a network probe,
   not a package install. Check `timedatectl status` for bad clock/TLS symptoms;
   never use `curl -k` to disguise certificate or clock failure. A captive portal,
   enterprise proxy or blocked site needs the site's authorized login/settings.

For the recorded site only, the diagnostic gateway binding is:

```bash
GATEWAY=192.168.1.1
ping -c 3 "$GATEWAY"
```

### 4.2 Reconnect an existing NM Wi-Fi profile

**ADMIN / NETWORK-DISRUPTIVE — Spark console.** Prefer an existing trusted profile.
From `nmcli connection show`, record its UUID and the previous active Wi-Fi UUID.
Use interactive input rather than embedding secrets or guessed UUIDs:

```bash
read -r -p 'Existing Wi-Fi profile UUID: ' WIFI_UUID
read -r -p 'Previously active Wi-Fi UUID (empty if none): ' PREVIOUS_WIFI_UUID
nmcli connection show uuid "$WIFI_UUID"
sudo nmcli --ask connection up uuid "$WIFI_UUID" ifname "$WIFI"
```

`--ask` collects missing credentials interactively; no password goes on the command
line or into shell history. This can change the active connection but should not
replace unrelated profiles. If the radio was off, record that fact before the
separately approved `sudo nmcli radio wifi on`. Repeat the layer checks above.

**Rollback:** if this activation displaced a prior connection,
`sudo nmcli --ask connection up uuid "$PREVIOUS_WIFI_UUID" ifname "$WIFI"` restores
it. If no prior connection existed, `sudo nmcli connection down uuid "$WIFI_UUID"`
returns to disconnected state without deleting the profile. Restore radio-off
with `sudo nmcli radio wifi off` **only if it was off before this procedure**.
Do not apply the empty-UUID branch as if it named a connection.

### 4.3 New personal Wi-Fi profile, only when none exists

**ADMIN / NETWORK-DISRUPTIVE — Spark console.** This recipe is for WPA2/WPA3
personal networks accepting `wpa-psk`; pure SAE, enterprise 802.1X and certificate
networks must use the existing desktop/NM connection editor with administrator-
supplied authentication policy. Do not downgrade network security to fit a recipe.

```bash
read -r -p 'Exact authorized SSID: ' SSID
WIFI_PROFILE="spark-internet-$(date -u +%Y%m%dT%H%M%SZ)"
sudo nmcli connection add type wifi ifname "$WIFI" \
  con-name "$WIFI_PROFILE" ssid "$SSID" \
  wifi-sec.key-mgmt wpa-psk ipv4.method auto ipv4.never-default no \
  ipv6.method auto connection.autoconnect yes
WIFI_UUID=$(nmcli -g connection.uuid connection show "$WIFI_PROFILE")
printf '%s\n' "$WIFI_UUID" > "$EVIDENCE/new-wifi.uuid"
sudo nmcli --ask connection up uuid "$WIFI_UUID" ifname "$WIFI"
```

Create only after confirming the generated profile name is unused and recording
any prior active connection as in 4.2. Leave password entry to `--ask`; do not use
`password SECRET`, shell variables containing passwords, or copied shared logs.
NM may store credentials in its protected system profile; that is private host
state and requires secure backup. Test address/route/DNS independently.

**Rollback:** reactivate the recorded previous profile if there was one, then
`sudo nmcli connection delete uuid "$WIFI_UUID"` removes **only the new profile**.
If still active, deleting it disconnects this Wi-Fi connection; use the console.
Restore the old radio state if changed. Never delete an existing profile during
an initial recovery attempt.

## 5. Persist Spark management Ethernet without stealing the default route

**Spark console — inspection first.** Select the correct host's address, not both:

```bash
MGMT_IF=enP7s7
MGMT_CIDR=10.10.10.2/24  # 66f1; use 10.10.10.3/24 on e8f1
ip -br link show dev "$MGMT_IF"
ip -4 address show dev "$MGMT_IF"
nmcli device show "$MGMT_IF"
nmcli connection show
```

If the persistent profile already has the correct unique address, no IPv4/IPv6
default route and autoconnect enabled, preserve it. Do not create another profile
merely because this runbook has an example. Confirm no competing inactive profile
is configured to autoconnect on this NIC. If the interface is bridged, bonded,
VLAN-managed, netplan-generated or already serving another purpose, **stop**;
changing that topology needs its own plan.

### New or corrected NM-owned standalone management profile

**ADMIN / NETWORK-DISRUPTIVE.** Keep the console and independent Wi-Fi session.
The example creates a new persistent profile, or clones the existing one so the
old address/configuration remains available for rollback. Record the UUID of the
existing authoritative standalone profile, including an inactive one; empty means
there truly is none. If several compete, resolve ownership before continuing.

```bash
read -r -p 'Existing management profile UUID (empty only if none): ' OLD_MGMT_UUID
MGMT_PROFILE="spark-management-$(date -u +%Y%m%dT%H%M%SZ)"
if [ -n "$OLD_MGMT_UUID" ]; then
  nmcli connection show uuid "$OLD_MGMT_UUID" > "$EVIDENCE/management-profile.before.txt"
  OLD_MGMT_AUTO=$(nmcli -g connection.autoconnect connection show uuid "$OLD_MGMT_UUID")
  sudo nmcli connection clone uuid "$OLD_MGMT_UUID" "$MGMT_PROFILE"
else
  OLD_MGMT_AUTO=
  sudo nmcli connection add type ethernet ifname "$MGMT_IF" \
    con-name "$MGMT_PROFILE" connection.autoconnect no
fi
NEW_MGMT_UUID=$(nmcli -g connection.uuid connection show "$MGMT_PROFILE")
printf '%s\n' "$OLD_MGMT_UUID" "$OLD_MGMT_AUTO" "$NEW_MGMT_UUID" \
  > "$EVIDENCE/management-rollback.txt"
sudo nmcli connection modify uuid "$NEW_MGMT_UUID" \
  connection.interface-name "$MGMT_IF" connection.autoconnect yes \
  ipv4.method manual ipv4.addresses "$MGMT_CIDR" ipv4.gateway '' \
  ipv4.routes '' ipv4.dns '' ipv4.dns-search '' \
  ipv4.never-default yes ipv4.ignore-auto-dns yes \
  ipv6.method disabled ipv6.never-default yes
if [ -n "$OLD_MGMT_UUID" ]; then
  sudo nmcli connection modify uuid "$OLD_MGMT_UUID" connection.autoconnect no
fi
sudo nmcli connection up uuid "$NEW_MGMT_UUID" ifname "$MGMT_IF"
```

The IPv6 policy here is for a dedicated IPv4-only management segment; do not apply
it to a site that depends on IPv6 management. Inspect the cloned profile *before*
activation for inherited policy routing, unusual route tables, cloned MACs or
802.1X requirements: the recipe is not a migration for those configurations.
Abort and delete only the newly created clone if it is unsuitable. The old profile
must remain intact except for the recorded autoconnect change.

Expected after activation: link up, exactly the approved management address,
connected subnet route via the management NIC, and the same working Internet
default route/DNS as before. Run section 4.1 again and section 6's cross-link checks.
A link can be administratively up without carrier; that is a cabling issue, not a
reason to add a gateway.

**Rollback at the console**, while these variables still contain the recorded
values (otherwise rebind them from `management-rollback.txt`):

```bash
if [ -n "$OLD_MGMT_UUID" ]; then
  sudo nmcli connection modify uuid "$OLD_MGMT_UUID" connection.autoconnect "$OLD_MGMT_AUTO"
  sudo nmcli connection up uuid "$OLD_MGMT_UUID" ifname "$MGMT_IF"
fi
sudo nmcli connection delete uuid "$NEW_MGMT_UUID"
```

If the old profile was inactive before the change, do not activate it during
rollback: restore its autoconnect setting and delete the new profile instead.
Record original active/inactive state before proceeding. Recheck routes/DNS after
rollback. Never substitute a broad `connection delete` or route flush.

## 6. Persist the Mac Ethernet side

**Mac — inspection.** Use the hardware/service mapping from section 2, then bind
the exact service name interactively, because it is site-specific:

```bash
read -r -p 'Verified dedicated Ethernet network service name: ' MGMT_SERVICE
MAC_MGMT_IF=en0  # replace if hardware-port mapping identified another device
networksetup -getinfo "$MGMT_SERVICE" | tee "$EVIDENCE/mac-ethernet.before.txt"
networksetup -getdnsservers "$MGMT_SERVICE" | tee "$EVIDENCE/mac-dns.before.txt"
networksetup -getsearchdomains "$MGMT_SERVICE" | tee "$EVIDENCE/mac-search.before.txt"
networksetup -listnetworkserviceorder > "$EVIDENCE/mac-service-order.before.txt"
route -n get default
route -n get 10.10.10.2
```

**ADMIN / NETWORK-DISRUPTIVE — Mac local console.** Use System Settings → Network →
the **verified dedicated Ethernet service** → Details → TCP/IP. Set Configure IPv4
**Manually**, IPv4 address `10.10.10.1`, subnet mask `255.255.255.0`, and leave
**Router blank**. This dedicated service must not supply DNS/search domains;
retain the Internet service's DNS. Preserve IPv6 policy unless the site has
explicitly selected an IPv4-only segment. Apply and retain the exact before-state,
including DHCP/manual mode, address, subnet, router, DNS, IPv6 and service order.
Do not set a dummy router, enable Internet Sharing, or change Wi-Fi configuration.
If the UI cannot express a blank router for this service, stop for the Mac network
administrator rather than inventing a gateway. Keep the Mac's Internet service
selected/working. Do not assume service order by itself prevents a bad default
route; inspect the actual route afterward.

**Rollback:** in the same service restore its recorded original IPv4 mode and
values (DHCP if it originally used DHCP), DNS/search values and any other setting
you changed, then Apply. Leave unrelated services untouched. If a new service was
created for a new adapter, remove only that explicitly identified new service;
never remove the physical hardware mapping or all network preferences. Verify the
original route and Internet connection return.

**Mac — inspection after either apply or rollback:**

```bash
ifconfig "$MAC_MGMT_IF"
route -n get 10.10.10.2
route -n get 10.10.10.3
route -n get default
scutil --dns
ping -c 3 10.10.10.2
ping -c 3 10.10.10.3
```

Expected: management destinations are directly connected through the chosen
Ethernet device; ordinary Internet still follows the original Internet path.
From each **Spark console**, `ping -c 3 10.10.10.1` tests the reverse direction.
One unreachable Spark while the other works calls for its own cable/address
inspection, not renumbering the whole LAN. ICMP filtering remains possible; SSH
in the next section establishes application reachability.

## 7. Establish SSH service and independent trust

### 7.1 Spark console: identify account, host key and service

**Inspection**, on each physical Spark as the intended login user:

```bash
id
hostname
systemctl status ssh.service --no-pager
systemctl status ssh.socket --no-pager
ss -ltn 'sport = :22'
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub -E sha256
cat /etc/ssh/ssh_host_ed25519_key.pub
```

Ubuntu may use `ssh.service` or socket activation. An absent/inactive unit is not
permission to disable the other model. Record current enabled/active states with
`systemctl is-enabled ssh.service` and `systemctl is-enabled ssh.socket`. Copy the
**public** host-key line and SHA256 fingerprint to a separately trusted operator
record using the physical console/removable media. Do not copy
`/etc/ssh/ssh_host_ed25519_key` (without `.pub`). Reinstalling an OS may legitimately
change the host key, but that requires fresh independent verification, not blindly
accepting a mismatch.

**ADMIN**, only on a new host with no SSH server installed, after Internet works:

```bash
sudo apt-get update
sudo apt-get install openssh-server
sudo /usr/sbin/sshd -t
```

Installing can start the service. If installed but not listening, inspect
`sudo journalctl -u ssh.service -u ssh.socket -n 80 --no-pager`, firewall policy and
`sudo /usr/sbin/sshd -T` before changing anything. Do not enable password/root login
or change `ListenAddress` merely to make a test pass. For a site deliberately using
service activation, and only when neither activation mode is already providing
SSH, the approved provisioning command is:

```bash
sudo systemctl enable --now ssh.service
```

**Rollback:** restore the recorded activation state. For a previously disabled,
inactive service that this step enabled, `sudo systemctl disable --now ssh.service`
returns it to that state. If installation newly introduced an active socket,
`sudo systemctl disable --now ssh.socket` closes that newly introduced activation
path too. Do this from the console only and never disable a pre-existing working
mode. Record package changes; there is no need to purge a package to close the
new listener, and no blind uninstall is prescribed. Firewall changes, if required,
must be a narrowly scoped site-approved rule for SSH from the management subnet,
with its exact owner-native reversal recorded first; do not disable the firewall.

### 7.2 Mac: choose an operator key; Spark console: append its public half

**Mac — local credential provisioning.** Reuse the correct existing operator key
if one is already authorized. Otherwise create a uniquely named dedicated key;
`ssh-keygen` prompts for a passphrase. Never overwrite an existing key.

```bash
umask 077
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
KEY="$HOME/.ssh/id_ed25519_spark_operator"
if [ -e "$KEY" ] || [ -e "$KEY.pub" ]; then
  printf '%s\n' 'Key already exists: inspect and deliberately reuse it; do not overwrite.'
else
  ssh-keygen -t ed25519 -f "$KEY" -C spark-operator
fi
ssh-keygen -lf "$KEY.pub" -E sha256
```

Transfer **only `$KEY.pub`** on trusted removable media to each Spark console, or
use an already independently authenticated administrator channel. Never move the
private key to the Spark, Git, a chat message or the model cache.

**Spark console — credential mutation**, logged in as the intended ordinary
account, not root. Bind the actual mounted public-key path, inspect that it is
exactly the single expected public key, and compare its fingerprint with the Mac:

```bash
read -r -p 'Absolute path to transferred operator public key: ' PUBLIC_KEY
ssh-keygen -lf "$PUBLIC_KEY" -E sha256
cat "$PUBLIC_KEY"
umask 077
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ -e "$HOME/.ssh/authorized_keys" ]; then
  cp -p "$HOME/.ssh/authorized_keys" "$EVIDENCE/authorized_keys.before"
else
  touch "$EVIDENCE/authorized_keys.was-absent"
fi
touch "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
if ! grep -Fqx -- "$(cat "$PUBLIC_KEY")" "$HOME/.ssh/authorized_keys"; then
  printf '\n' >> "$HOME/.ssh/authorized_keys"
  cat "$PUBLIC_KEY" >> "$HOME/.ssh/authorized_keys"
  printf '\n' >> "$HOME/.ssh/authorized_keys"
fi
```

Stop before writing if `.ssh`/`authorized_keys` is a symlink, has unexpected
ownership, the transferred file has multiple keys/options, or policy requires
source restrictions. Review any existing restricted entry for the same key:
do not add an unrestricted duplicate that bypasses it. This procedure preserves
existing keys; it does not replace the file with a single key. Confirm ownership
belongs to the login account with `stat` and that the home directory is not
world-writable.

**Rollback:** remove only the exact newly appended public-key line while at the
console. Restore `authorized_keys.before` only if no other keys have changed
since the snapshot; otherwise merge the one-line removal. If the file was absent,
remove the new file only if it still contains no other authorized keys. Do not
remove a key that was already present. Keep the Mac private key until all intended
authorizations are reviewed; deleting it alone does not revoke a public key.

### 7.3 Mac: aliases and strict first connection

Back up an existing `~/.ssh/config` and the chosen known-hosts file into the private
`$EVIDENCE` directory before editing. Use the local editor, preserving unrelated
entries and Includes. Specific entries must precede any broad `Host *` rules that
would otherwise take precedence. Add or update exactly these **recorded-site**
entries only after confirming both addresses and host identities:

```sshconfig
Host spark-66f1-wired
    HostName 10.10.10.2
    User statsparrot
    IdentityFile ~/.ssh/id_ed25519_spark_operator
    IdentitiesOnly yes
    StrictHostKeyChecking yes
    HostKeyAlgorithms ssh-ed25519
    ForwardAgent no

Host spark-e8f1-wired
    HostName 10.10.10.3
    User statsparrot
    IdentityFile ~/.ssh/id_ed25519_spark_operator
    IdentitiesOnly yes
    StrictHostKeyChecking yes
    HostKeyAlgorithms ssh-ed25519
    ForwardAgent no
```

If reusing a different key, use its actual path. These are Mac operator aliases,
not replacement Spark-to-Spark controller configuration. Before connecting,
place each **console-verified public host key** in `~/.ssh/known_hosts`: each record
is the literal destination IP, one space, then the complete `ssh-ed25519` public-key
line from that host. Use the editor to merge, not overwrite; do not paste an example
fake key. Inspect existing entries first:

```bash
ssh-keygen -F 10.10.10.2 -f "$HOME/.ssh/known_hosts"
ssh-keygen -F 10.10.10.3 -f "$HOME/.ssh/known_hosts"
```

If the file does not yet exist, create it with mode `600`. Missing entries can be
added from the independently verified public records; conflicting entries require
investigation. `ssh-keyscan` may collect a candidate key but **does not establish
trust**. Do not use `StrictHostKeyChecking=no`, blindly answer yes, or erase all
known-hosts records to resolve a conflict. Verify `ssh -G` below has no unexpected
`ProxyJump`, `ProxyCommand`, `HostKeyAlias` or alternate `UserKnownHostsFile`; broad
user/system configuration may alter these routes. Resolve such conflicts locally
before connecting.

**Mac — inspection/authentication, no model calls:**

```bash
ssh -G spark-66f1-wired
ssh -G spark-e8f1-wired
ssh -o ConnectTimeout=10 -o ControlMaster=no -o ControlPath=none \
  spark-66f1-wired 'hostname; id -un; uname -m'
ssh -o ConnectTimeout=10 -o ControlMaster=no -o ControlPath=none \
  spark-e8f1-wired 'hostname; id -un; uname -m'
```

Expected respectively: `spark-66f1`/`spark-e8f1`, `statsparrot`, `aarch64`, with strict
host-key checking succeeding. A passphrase prompt is for the **local** private key.
Once the operator key is unlocked in the Mac's approved agent/session, repeat with
`-o BatchMode=yes` to prove unattended authentication, if required by the controller.
Do not remove the passphrase or copy private keys merely to bypass that gate.
Failure in BatchMode means automation is not ready even if interactive SSH works.

**Rollback of aliases/trust:** restore only the edited entries from their before-
snapshots, preserving concurrent additions. If trust must be revoked, remove only
the verified affected host's record after investigating; do not delete the entire
known-hosts file. Neither Git checkout nor controller installation recreates these
Mac keys, authorized keys, SSH config or host identity files.

## 8. Rediscover a disconnected Spark without trusting its name

Start at the console if possible: `ip -br -4 address`, `hostname`, the host-key
fingerprint and router DHCP lease page are stronger evidence than an old lease.
On the Mac, `.local` names are optional independent LAN/Wi-Fi alternatives:

```bash
dns-sd -G v4v6 spark-66f1.local
dns-sd -G v4v6 spark-e8f1.local
```

These commands continue watching; press Ctrl-C after recording answers. No answer
can mean mDNS is absent or multicast is isolated, not that the host is dead. mDNS
usually does not cross routed networks. Check the router's authenticated DHCP
client list against the Spark's console-observed Wi-Fi MAC/hostname; leases may
change. Do not treat `192.168.1.31` or `.18` as permanent endpoints, and do not
reuse another device's lease.

For any discovered address, inspect the Mac's route to it and compare the SSH host
key to the independent console pin **before** logging in. If approved as an
alternate management route, create a separate explicit alias (for example a LAN
alias) with the same user/key and strict trust, rather than silently redirecting
`spark-*-wired` to Wi-Fi. Approve any address reservation in the router's existing
DHCP system; keep its before-state for rollback. A name resolving to Tailscale,
a jump host or the wrong subnet is not proof of independent wired reachability.

## 9. Persistence and maintenance-window reboot acceptance

Do not reboot a live serving host to complete this checklist. First inspect saved
configuration independently of live state:

- NM: `nmcli connection show uuid "$NEW_MGMT_UUID"` (or the existing approved
  UUID) must show the intended interface/address, `connection.autoconnect: yes`,
  empty management gateway, and `ipv4.never-default: yes`. The intended Wi-Fi
  profile must remain persistent/autoconnect according to site policy.
- A netplan/provisioning owner must contain those intended settings in its
  authoritative files, not just an ephemeral live `ip address add` result.
- Mac: reopen the selected Ethernet service and verify saved manual address,
  subnet and blank Router; verify the Internet service remains configured.
- SSH: inspect the chosen service/socket enabled state and make a **new** SSH
  connection while keeping the old session/console open. A multiplexed old
  connection is not evidence the new authentication configuration works.
- Record current Internet, management and fabric routes separately. Preserve
  host keys, SSH config and public trust records outside the disposable checkout.

Only in an independently approved maintenance window, with workloads safely
stopped through their owning workflow and a human at the console, perform a
controlled reboot using the site's shutdown procedure. No reboot is authorized
by this document. After that future reboot, repeat address/carrier, default route,
DNS/TLS, saved-profile and fresh trusted-SSH checks on **each** host. Record actual
results and time; until then label persistence **configured, reboot unverified**.
If addresses disappear, inspect owner/autoconnect/order/adapter mapping rather
than reapplying ad-hoc addresses forever. Use the earlier rollback at the console
and the emergency runbook if the owner does not restore the intended state.

## 10. Reconnect decision tree

| Symptom | Inspect in order | Safe action / stop condition |
| --- | --- | --- |
| No display/login/power | Power indicator, approved power supply, display input, local account access | Obtain physical/vendor recovery access; do not factory-reset or cycle a working worker blindly |
| No Internet, Ethernet SSH works | Wi-Fi radio/SSID → lease → default route → gateway → DNS → clock/TLS | Repair only the existing Internet owner; do not assign an Internet gateway to management or fabric |
| No Internet and no SSH | Local console identity, owner and section 4 | Recover Internet and management independently; external DNS changes cannot fix absent carrier |
| Mac cannot reach either management address | Mac adapter/service mapping → carrier/switch/cables → address/mask → direct route | Restore recorded Mac service or cable path; no default-route flush |
| One Spark has no Ethernet | That cable/socket/NIC carrier → unique address/mask → owner/profile | Console repair on that host only; retain Wi-Fi fallback |
| Ping works, SSH times out | `ss -ltn` and SSH units at console; route and narrowly scoped firewall policy | Start only the intended service mode or seek firewall approval; do not disable firewall globally |
| SSH connection refused | Correct host/IP first, then listener/service logs | It is not a key-install failure; do not regenerate keys |
| SSH says permission denied | Correct user, public key, permissions, `ssh -v`, server auth logs | Preserve other authorized keys; never enable root/password login as a shortcut |
| SSH host key changed | Physical console key and device identity, DHCP/duplicate-address history | Stop. Re-pin only after independent re-verification and an explained legitimate change |
| `.local` fails, wired IP works | mDNS/network isolation | Keep verified wired alias; mDNS is optional, not a cluster prerequisite |
| Fabric/serving endpoint fails, management works | Exact saved endpoint and destination route, then multi-node/recovery playbook | Do not restart GPU work or add routes here; management success does not qualify serving |
| Repair works until reboot | Authoritative saved profile, autoconnect conflicts, netplan/cloud-init owner, adapter mapping | Restore persistence through the existing owner; label reboot acceptance pending until actually observed |

## 11. Offline handoff before models or recovery automation

Keep this runbook and the emergency runbook locally readable on the Mac **and** on
operator-controlled offline media. The operator must be able to open them without
SSH, a cloud login, GitHub or the model. Prepare the detailed backup/restore set in
[SPARK_RECOVERY_RUNBOOK.md](SPARK_RECOVERY_RUNBOOK.md), not just this checklist:

- Physical cable/port map, site address allocation, NIC MACs, router/DHCP access
  method, console/account/admin access method and the secure credential escrow
  location. Never put passwords or private keys into the runbook or Git.
- Independently verified public host keys/fingerprints, Mac aliases, and the
  permitted controller/source-address policy. Private keys stay on their origin
  hosts or in separately encrypted, access-controlled disaster-recovery escrow.
- A **fresh trusted source checkout** (`SOURCE`), full pinned `REVISION`, source
  bundle, installer, locked dependency/offline materials, and secure host-config
  backups. Do not reset a live checkout to make it match. The recorded code pin
  `65b05d5503e6ffefd2f07d07ebe86333917d5086` is an example of the installed Mac
  revision, not a claim this documentation is included in that older commit.
- Absolute private inventory path (`INVENTORY`), independently verified public
  trust JSON path (`TRUST`), and the exact saved plans/receipts required by the
  emergency procedure. JSON trust shape is `version: 1`, `nodes`, and per-node
  `host_key`; optional `management` entries contain `alias`/`address`, and
  `source_addresses` declares approved source addresses. Every optional management
  address must also appear in that node's approved source addresses, and trust
  must pin every inventory member. Obtain real public keys from console evidence,
  not invented values or an untrusted SSH query.
- Inventory uses actual SSH aliases and can declare independent
  `management.ssh_targets`. Optional `serving` has `address` and `interface`.
  Neither field configures the OS, installs keys, qualifies serving nor proves
  reachability; do not rewrite an existing saved deployment plan after a network
  repair. Peer-controller trust setup is in the multi-node runbook.
- Model snapshots, their manifests and runtime images are **not** included merely
  because source is in Git. Cache presence is not full weight-hash validation.
  Preserve exact plan/artifact evidence and use the linked model procedures.
- Record trusted-SSH success from a new session for each node, Internet routing
  and DNS separately, management independence from fabric, and any reboot checks
  still unperformed. No fabricated success receipts.

The follow-on runbooks bind their own shell variables before use:
`PREFIX=$HOME/projects/local-llm-stack-cluster`, `RELEASE=$PREFIX/releases/$REVISION`,
`PY=$RELEASE/.venv/bin/python`, `PLAN`, canonical **full-plan** `PLAN_SHA256` (not a
raw file hash), and a new private `EVIDENCE` directory. Do not execute a serving
command with unbound paths. Manual saved-plan activation requires the trusted
`--plan-sha256` and fresh `--output`; an active recovery fence requires its owning
recovery workflow, not manual force, file deletion or a replacement plan.

### Sources and boundaries

Repository sources for this procedure: [README](../README.md),
[original setup runbook](spark-setup-runbook.md),
[network/port explanation](networking-and-ports.md),
[cluster foundation and peer trust](CLUSTER.md),
[inventory](../cluster/inventory.json),
[inventory validation](../tools/spark_cluster/config.py),
[peer trust parser](../scripts/configure-spark-peer-ssh.py),
[fabric MTU guards](../scripts/configure-spark-fabric-mtu.sh),
[GLM fabric description](GLM53_TP.md), and
[recorded rollout gates](LOCAL_FIRST_OPERATIONS_PLAN.md).
CLI references are the installed standard help/man pages for `nmcli`, `ip`,
`networksetup`, `ssh`, `ssh-keygen`, `systemctl` and `netplan`; inspect those before
using a different OS/version. For NetworkManager properties see the
[official connection-setting reference](https://networkmanager.dev/docs/api/latest/nm-settings-nmcli.html).
Site-owned decisions that cannot be inferred from this repository are the real
physical socket/switch map, current profile owner and masks, SSID/authentication,
address conflicts, firewall/proxy policy, admin access and trusted key material.
The flow deliberately stops for these facts rather than guessing them.
