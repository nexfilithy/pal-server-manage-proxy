use std::{env, fs, io, net::SocketAddr, path::{Path, PathBuf}, process::{Child, Command, Stdio}, sync::{atomic::{AtomicU64, Ordering}, Arc}, time::{Duration,Instant}};
use chrono::{Local, Timelike, Days};
use dotenv::dotenv;
use log::LevelFilter;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle, time::sleep};
use fxhash::FxHashMap;
use simple_logger::SimpleLogger;
use walkdir::WalkDir;
use simple_eyre::eyre::{eyre, Result, WrapErr};

#[derive(Debug)]
struct UdpProxyConn {
    pub sock: UdpSocket,
    pub last_activity: AtomicU64,
}

impl UdpProxyConn {
    fn new(sock: UdpSocket) -> Self {
        Self {
            sock,
            last_activity: AtomicU64::new(0),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let start = Instant::now();
    SimpleLogger::new().with_level(get_debug_level(env::var("LOG_LEVEL").unwrap_or_else(|_| {println!("log level was incorectly supplied using default = info");"info".to_string()}))).init().unwrap();
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| {log::warn!("listen address was not supplied using default palworld port +1"); "0.0.0.0:8212".to_string()});
    let listener = Arc::new(UdpSocket::bind(&listen_addr).await?);
    log::info!("listening on: {}", listen_addr);

    let mut sockets: FxHashMap<SocketAddr, (Arc<UdpProxyConn>, JoinHandle<()>)> = FxHashMap::default();
    let mut buf: [u8; 65536] = [0; 65536];
    let mut gameserver_process = Option::None;
    let server_update_hour = env::var("SERVER_UPDATE_HOUR").unwrap_or_default().parse().unwrap_or_default();
    let mut updatetime = update_time(server_update_hour);
    let mut servertimeout = start.elapsed().as_millis() as u64;
    let (tx, mut rx) = mpsc::channel::<SocketAddr>(128);
    let servertimeoutvar = env::var("SERVER_TIMEOUT").unwrap_or_else(|_| "300000".to_owned()).parse::<u64>().unwrap();

   loop {
    let timeout = sleep(Duration::from_millis(15000));
    let update = sleep(updatetime);
    tokio::select! {

        addr = rx.recv() => {
            if let Some(addr) = addr {
                if let Some((_conn, handle)) = sockets.remove(&addr) {
                    log::info!("closing {addr} due to inactivity");
                    handle.abort();
                    servertimeout = start.elapsed().as_millis() as u64;
                    updatetime = update_time(server_update_hour);
                }
            }

        }
        ret = listener.recv_from(&mut buf) => {
            let (read, addr) = ret.wrap_err("failed to accept connection")?;
            if !gameserver_process.is_some() {
                match run_game_server().await{
                    Ok(child) => gameserver_process = Some(child).into(),
                    Err(e) => log::warn!("starting game server failed with {e}")
                };
            }


            if let Err(why) = handle_connection(
                listener.clone(),
                addr,
                &mut buf[..read],
                &mut sockets,
                tx.clone(),
            )
            .await
            {
                log::error!("{why:#}");
            }
        }
        _ = timeout => {
            let timenow = start.elapsed().as_millis() as u64;
            if sockets.is_empty() && timenow - servertimeout > servertimeoutvar {
                match gameserver_process.take() {
                    Some(child) => {
                        stop_game_server(child);
                        match backup_save() {
                            Ok(_) => log::info!("back up ok"),
                            Err(e) => log::warn!("back up not ok {e}")
                        }
                    },
                    None => {updatetime = update_time(server_update_hour); log::debug!("current duration for update is {:?} s", updatetime)}
                }
        }
        }
        _ = update => {
            if sockets.is_empty() && gameserver_process.is_none() {
                log::info!("starting to update server");
                update_server();
                log::info!("finished to update server");
                updatetime = update_time(server_update_hour)
            };
        }
    }
   }
}
    
async fn handle_connection(
    src: Arc<UdpSocket>,
    addr: SocketAddr,
    buffer: &mut [u8],
    sockets: &mut FxHashMap<SocketAddr, (Arc<UdpProxyConn>, JoinHandle<()>)>,
    tx: mpsc::Sender<SocketAddr>,
) -> Result<()> {

    
    let src_addr = src.local_addr().unwrap();
    let socket = match sockets.get(&addr) {
        Some((socket, _handle)) => {
            socket.last_activity.fetch_add(1, Ordering::SeqCst);
            socket.clone()
        }

        None => {
            log::info!("[new conn] [origin: {addr}] [src: {src_addr}]");


            let socket = {
                let sock = UdpSocket::bind("0.0.0.0:0").await?;
                Arc::new(UdpProxyConn::new(sock))
            };
            let close_after = Duration::from_millis(env::var("CONNECTION_TIMEOUT").unwrap().parse::<u64>().unwrap_or_default());
            let src_clone = src.clone();
            let socket_clone = socket.clone();
            let handle = tokio::spawn(async move {
                if let Err(why) = traffic(addr, src_addr, src_clone, socket_clone).await {
                    log::error!("{why:#}");
                };
            });
            tokio::spawn(close_after_inactivity(
                addr,
                close_after,
                tx.clone(),
                socket.clone(),
            ));

            sockets.insert(addr, (socket.clone(), handle));
            socket
        }
    };

    match socket.sock.send_to(buffer, env::var("SERVER_ADDR").unwrap_or_else(|_| {log::warn!("server adress not supplied using default palworld port on localhost"); "127.0.0.1:8211".to_string()})).await {
        Ok(size) => {
            log::trace!("from [{}] to [{}], size: {}", src_addr, addr, size);
            Ok(())
        }
        Err(err) => Err(err).wrap_err("failed to write data to the upstream connection"),
    }
}

async fn traffic(
    addr: SocketAddr,
    src_addr: SocketAddr,
    src: Arc<UdpSocket>,
    socket: Arc<UdpProxyConn>,
) -> Result<()> {
    let mut buffer = [0u8; 65536];

    loop {
        let read_bytes = socket.sock.recv(&mut buffer).await?;
        let sent_bytes = src.send_to(&buffer[..read_bytes], addr).await?;
        if sent_bytes == 0 {
            return Err(eyre!("couldn't sent anything to downstream"));
        }
        log::trace!("from [{}] to [{}], size: {}", addr, src_addr, sent_bytes);

        socket.last_activity.fetch_add(1, Ordering::SeqCst);
    }
}

async fn close_after_inactivity(
    addr: SocketAddr,
    close_after: Duration,
    tx: mpsc::Sender<SocketAddr>,
    socket: Arc<UdpProxyConn>,
) {
    let mut last_activity = socket.last_activity.load(Ordering::SeqCst);
    loop {
        tokio::time::sleep(close_after).await;
        if socket.last_activity.load(Ordering::SeqCst) == last_activity {
            break;
        }
        last_activity = socket.last_activity.load(Ordering::SeqCst);
    }

    if let Err(why) = tx.send(addr).await {
        log::error!("couldn't send the close command to conn channel: {why}");
    }
}

async fn run_game_server() -> io::Result<Child> {
    log::info!("Starting game server...");
    let path_binary = env::var("PATH_SERVER_BINARY").expect("Server binary path is missing");
    Command::new(path_binary)
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
}

fn stop_game_server(mut child: Child) {
    log::info!("Stopping game server...");

    match child.kill() {
        Ok(_) => {
            let _ = child.wait();
            log::info!("Game server stopped.");
        },
        Err(e) => log::warn!("Failed to stop game server: {}", e),
    }    
}

fn backup_save() -> std::io::Result<()> {
    let env_parent = env::var("PATH_SERVER_SAVEFILES").expect("Savefile folder path is missing");
    let env_backup = env::var("PATH_SAVEFILES_BACKUP").expect("Savefile backup path is missing");
    let parent_dir = Path::new(&env_parent);
    let backup_dir = Path::new(&env_backup);
    fs::create_dir_all(&backup_dir)?;

    for entry in WalkDir::new(parent_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if is_specific_file(&path) {
            let relative_path = path.strip_prefix(parent_dir).unwrap();
            let file_stem = relative_path.file_stem().unwrap().to_str().unwrap();
            let extension = relative_path.extension().unwrap().to_str().unwrap();

            // Generate a timestamp for the backup file
            let timestamp = chrono::Local::now().format("%Y%m%dT%H%M%S").to_string();
            let backup_filename = format!("{}_{}.bak", file_stem, timestamp);
            let destination_path = backup_dir.join(relative_path.with_file_name(backup_filename));

            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }

            fs::copy(&path, &destination_path)?;
            log::info!("File backed up to {:?}", destination_path);

            // Clean up older versions, keeping the last 5
            clean_up_old_versions(backup_dir, file_stem, extension, 5)?;
        }
    }
    Ok(())
}

// Placeholder function to determine if a file meets your specific criteria
fn is_specific_file(path: &Path) -> bool {
    // Example condition: file extension is "txt"
    path.extension().and_then(|ext| ext.to_str()) == Some("sav")
    // Add more conditions as needed
}

fn clean_up_old_versions(parent: &Path, stem: &str, ext: &str, keep_last: usize) -> std::io::Result<()> {
    let backups_dir = parent;
    let pattern = format!("{}_*.bak", stem);

    let mut backups: Vec<PathBuf> = WalkDir::new(backups_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map_or(false, |s| s.starts_with(&pattern) && s.ends_with(ext)))
        .map(|e| e.into_path())
        .collect();

    // Sort backups by their modified time or filename in descending order
    backups.sort_by(|a, b| b.cmp(a));

    // Keep the last 5 versions, remove the rest
    for old_backup in backups.iter().skip(keep_last) {
        fs::remove_file(old_backup)?;
        log::info!("Removed old backup: {:?}", old_backup);
    }
    Ok(())
}

fn update_server() {
    let mut update = Command::new("steamcmd")
    .arg("+login")
    .arg("anonymous")
    .arg("+app_update")
    .arg("2394010")
    .arg("validate")
    .arg("+quit")
    // .stdout(Stdio::null())
    // .stderr(Stdio::null())
    .spawn()
    .unwrap();
    let _ = update.wait();
}

fn update_time(hour:u32) -> Duration {
    let now = Local::now().naive_local();
    let next_3am = if now.hour() >= 3 {
        now.date().checked_add_days(Days::new(1)).unwrap().and_hms_opt(hour,0,0)
    } else {
        now.date().and_hms_opt(hour,0,0)
    };
    let sleep_time = (next_3am.unwrap() - now).to_std().unwrap();
    log::debug!("current time to update is {:?} ", sleep_time);
    sleep_time
} 

fn get_debug_level(args:String) -> LevelFilter {
    match args.to_lowercase().as_str() {
        "off" => {println!("using log level off");LevelFilter::Off},
        "error" => {println!("using log level error");LevelFilter::Error},
        "warn" => {println!("using log level warn");LevelFilter::Warn},
        "info" => {println!("using log level Info");LevelFilter::Info},
        "debug" => {println!("using log level Debug");LevelFilter::Debug},
        "trace" => {println!("using log levle Trace");LevelFilter::Trace},
        _ => {println!("log level was incorectly supplied using default = info"); LevelFilter::Info}
    }
} 