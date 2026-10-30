# Phase Three runner: install and pilot guide

The runner is one Python file (standard library only) plus a page. It runs on the gateway, launches the fault scripts itself, takes independent probe readings, and stores every event and trial in SQLite.

## 1. Copy it over

Copy this folder to the gateway, for example to `/opt/cwn-runner` (runner.py, ui.html, scenarios.json). Check the tools it needs:

    python3 --version          # 3.10 or newer (Ubuntu 22.04 has 3.10)
    which ping tc dig ssh      # install dig with: sudo apt install dnsutils

## 2. Passwordless SSH (check this one carefully)

The runner runs as root, and so do the fault scripts it launches, so **root's** key is the one that must be trusted by AP2 and by the station. A key that works for your normal user is not enough. Test each host exactly as the runner will use it:

    sudo ssh -o BatchMode=yes root@192.168.1.101 true && echo AP2 ok
    sudo ssh -o BatchMode=yes takondwa@192.168.1.11 true && echo station ok

If a test fails, find root's public key (create one if there is none) and add it to that host from your normal user, whose SSH already works:

    sudo ls /root/.ssh/                                    # look for id_ed25519.pub or id_rsa.pub
    sudo ssh-keygen -t ed25519 -N '' -f /root/.ssh/id_ed25519   # only if root has no key
    sudo cat /root/.ssh/id_ed25519.pub | ssh takondwa@192.168.1.11 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'

Use the real key filename in the third command, then repeat the test.

## 3. Tell it where things are

- Fault scripts: the repository's `faults_commands` folder. Run the runner with `--fault-dir /path/to/faults_commands` (use the full path; `~` means /root when running as root).
- WAN interface: it is now `enx00e04c507970`. If the adapter is ever replaced, change `wan_iface` in the `runner` block of `scenarios.json`. That one setting feeds every command and probe that uses `{wan_iface}`.
- Copy `ap_outage_test.sh` (new, for the availability scenario) into the same `faults_commands` folder and make the scripts executable: `chmod +x *.sh *.bash`.

## 4. Dry run first (nothing is executed)

    python3 runner.py --simulate --speed 10

Open the printed link in your browser. This runs the whole sequence against a fake testbed so you can learn the screens. Windows are long at normal speed, so lower them under "Pilot settings" for the practice.

## 5. Real run

    sudo python3 runner.py --fault-dir /path/to/faults_commands

It prints a link such as `http://0.0.0.0:8080/?t=TOKEN`. Open it from your laptop using the gateway's LAN address, for example `http://192.168.1.1:8080/?t=TOKEN`. If the firewall is on:

    sudo ufw allow from 192.168.1.0/24 to any port 8080 proto tcp

Connect your laptop to AP1 or by cable, never to AP2, because WIFI-FLAP and device-availability faults would cut you off.

To keep it running and restart it after a reboot, create `/etc/systemd/system/cwn-runner.service`:

    [Unit]
    Description=Phase Three trial runner
    After=network-online.target

    [Service]
    WorkingDirectory=/opt/cwn-runner
    ExecStart=/usr/bin/python3 /opt/cwn-runner/runner.py --fault-dir /path/to/faults_commands
    Restart=on-failure
    User=root

    [Install]
    WantedBy=multi-user.target

Then `sudo systemctl daemon-reload && sudo systemctl enable --now cwn-runner` and read the link with `journalctl -u cwn-runner -n 5`. The token is also saved in `data/token.txt`.

## 6. What happens when you press Start (automatic mode)

1. Preflight: both hosts reachable, no leftover fault process, no netem rule on the WAN interface. If any check fails the trial does not start and says why.
2. Baseline: the probes record the pre-fault readings for the baseline period.
3. Launch: the runner starts the fault script. The command time is recorded.
4. Onset: the probes confirm the fault is applied. That moment starts the clock. If it is not confirmed within 90 s the trial becomes an error and the fault is cleared.
5. Window: readings every few seconds. The participant's report is the hand button.
6. Clear: the stop command runs, then the probes confirm it is gone (one automatic retry).
7. Recovery: waits the minimum time and checks the probes are back near baseline.
8. You enter what they named and whether the description was right, then Save.

Manual mode keeps the old buttons for any scenario or any moment automatic mode misbehaves. The probes still record in manual mode.

## 7. Safety net

- On every start the runner runs all stop and clear commands, so a crash or reboot cannot leave a fault running.
- "Clear all faults now" does the same on demand (not while a fault is starting or active).
- A trial that was running when the runner restarted is saved as invalid.

## 8. Things to confirm in the pilot (one dry trial per scenario, no participant)

1. The fault scripts run without asking questions, and the number passed as the last argument is **minutes** for cpufill, dns, WAN-loss and latency. The runner passes the window plus one minute. If a script means something else, change `launch_cmd` in `scenarios.json`.
2. The DNS script writes `/tmp/fault_dns.pid` on the station, and the flap and CPU scripts write theirs on AP2. If a path differs, change `probe_defs`.
3. `cat /sys/class/net/phy0-ap0/carrier_down_count` works on AP2 (the code falls back to `carrier_changes`).
4. The DNS trial records `dns_fail_pct`, the same `node_dns_failure_ratio{job="gw-latency"}` series the dashboard panel uses, read from Prometheus (set `prometheus_url` in `scenarios.json` if it is not `http://localhost:9090`). It is only recorded, it does not gate anything; onset and clear are decided by `dns_pid`.
5. Onset thresholds look right in the live probes table: CPU at least 80 per cent, packet loss and latency by the netem rule. Change them under `onset` in `scenarios.json` if needed.
6. Probes run about every 3 to 5 seconds, so verified onset can sit up to that much after the real start. This is the same for both conditions.
7. AVAIL (AP outage) is new: it takes AP1's mesh port out of the br-lan bridge for 30 s, so AP2 stops answering, then puts it back. First try by hand on AP1: `ip link set dev phy1-mesh0 nomaster`, check that the gateway can no longer ping 192.168.1.101, then `ip link set dev phy1-mesh0 master br-lan` and check it answers again. Do it from the wired side of AP1 only. BW-SAT uses `bw_sat_test.sh` (local iperf3 from the station to the gateway): install iperf3 on both (`sudo apt install iperf3`), copy the script into the fault folder, and edit the rate in its launch command (default 20 Mbit/s). MEMORY needs the new `memfill_test.sh` (it now writes `/tmp/fault_memfill.pid` on AP2), and LOSS-JITTER uses the new `loss_jitter_test.sh` (netem loss plus jitter on the gateway WAN). Copy all the `.sh` files from this zip into the fault folder and `chmod +x` them. Only TRAFFIC-MIX still runs in manual mode, until its script is written.

## 9. Data

Download the trials CSV, the events CSV and the probe samples CSV (one row per probe reading per trial) at the end of each session, plus the JSON backup. The database file is `data/trials.db`.


## Saving time: ending a window or recovery early

Both are off by default, so every trial runs its full planned lengths. In the page, open "Pilot settings":
- **End the active window early once the participant has reported:** after the first report plus the grace seconds you set (default 5), the window ends and the fault is cleared. Later reports in that trial are not captured.
- **Do not wait for the full recovery time once the testbed is verified normal:** the trial finishes once the probes have shown the testbed normal for the settle seconds you set (default 15).
- **End window now** (while ACTIVE) and **Skip the recovery wait** (while RECOVERY, automatic trials only) do the same for a single trial by hand. Skipping is refused until the probes show the testbed normal.

Each trial row records `window_actual_s`, `window_ended_early`, `recovery_actual_s` and `recovery_ended_early`. Use one setting for every participant and condition, and report it in the methods.


## What is recorded in every trial

Whatever the scenario, the runner samples these every few seconds: AP2 and AP1 CPU and memory, AP1 and AP2 reachability (three pings each from the gateway), gateway RTT, loss and jitter (three pings from the gateway), the DNS failure ratio, AP1 eth0 upload and download throughput, and the dashboard's own QoE RTT P90 and loss ratio (the last four are read from Prometheus at `prometheus_url`). Scenario-specific checks (process and interface checks) are added on top. Each trial row summarises them in `probe_summary`, and `samples.csv` has every sample. If Prometheus is unreachable those columns stay empty and the trial is otherwise unaffected.


## Downloading the data

"Download all data (Excel)" gives one `.xlsx` workbook with three sheets (trials, events, samples), proper columns and numbers, whatever your regional settings. The CSV buttons give plain comma-separated files with a UTF-8 marker; if your spreadsheet shows everything in one column (some regional settings expect semicolons), use "Trials CSV (semicolon)" or the Excel download. In pandas: `pd.read_csv(path)` for the comma files.


## Extra fields in each trial row

`observer` (set in Session setup), `dashboard_version` (set per condition under Pilot settings, so fill it in before the first session), `task_order_code` (the randomised order, separated by `|`) and `order_seed`, `onset_method` (probe, manual_probe_confirmed, manual_forced or catch_window), `report_times_s` (every report press, in seconds after onset, space separated) and `participant_description` (the participant's own words, typed in the outcome box). The Excel download also has an `indicator_map` sheet that links each runner scenario to the instrument task (T1 to T10) and has an empty column for the Phase Two criticality rating. The task mapping there is a suggestion for you to confirm.


## Dashboard-view probes (d_ prefix)

Every sample now also records what each dashboard indicator shows, using the same 1 minute PromQL as the dashboards: `d_clients`, `d_rtt_p90`, `d_jitter`, `d_inet_loss`, `d_dns_fail`, `d_up_mbps`, `d_down_mbps`, `d_data_24h_gb`, `d_chan_util`, `d_mesh_snr`, `d_mesh_rssi`, `d_mesh_loss`, and for each AP `d_apN_up`, `d_apN_cpu`, `d_apN_mem`, `d_apN_wifi_loss`, `d_apN_flaps`, `d_apN_min_snr`. They are read from Prometheus (`prometheus_url` in scenarios.json) every `sample_s` seconds, so they appear in the samples export beside the independent ground-truth probes. An empty value means Prometheus returned nothing at that moment (for example `d_apN_min_snr` with no client connected, or the mesh metrics while AP2 is unreachable). Top Applications by Data Volume is a list rather than a single number, so it is not sampled.
