use clap::{Parser};
use rocket::{get, routes, tokio, State, response::status};
use std::{
    sync::{Arc, Mutex},
    path::PathBuf,
    time::{Duration, Instant},
};
use serde::{Deserialize, Serialize};
use url::Url;
use rocket::serde::json::Json;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;


/// Simple display of IP leases from ISC DHCPD
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Servers file
    #[arg(short, long)]
    servers: PathBuf,
    /// Base URL for this proxy (used for cache)
    #[arg(short, long)]
    base_url: Url,
    /// URL of servers.json to merge with
    #[arg(short, long)]
    url: Option<Url>,
    /// Enable trace logging
    #[arg(short, long)]
    debug: bool,
}

#[derive(Deserialize, Serialize, Clone)]
struct ServerEntry {
    name: String,
    address: Url,
}

#[derive(Deserialize, Serialize, Clone)]
struct Servers {
    servers: Vec<ServerEntry>,
    cache: Option<String>,
}

struct ServerCacheGated {
    my_servers: Servers,
    merged_servers: Servers,
    digest: String,
    last_update: Instant,
}

struct ServerCache {
    cache: Arc<Mutex<ServerCacheGated>>,
}

fn merge_servers(upstream_servers: &Servers, my_servers: &Servers) -> (Servers, String) {
    let mut merged_servers = upstream_servers.servers.clone();
    merged_servers.append(&mut my_servers.servers.clone());
    let servers = Servers {
        servers: merged_servers,
        cache: None,
    };
    let mut digest = sha256::digest(serde_json::to_string_pretty(&servers).expect("Failed to serialize merged list"));
    digest.truncate(8);
    digest.push('\n');
    (servers, digest)
}

static DEFAULT_SERVERS_URL: &'static str = "https://servers.sidestore.io/servers.json";

#[get("/servers.json")]
async fn servers(
    server_state: &State<ServerCache>,
    send_channel: &State<tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>>
) -> Json<Servers> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    send_channel.send(tx).expect("Failed to send request message");
    rx.await.expect("Failed to receive response message");
    let server_state = server_state.cache.lock().unwrap();
    Json(server_state.merged_servers.clone())
}

#[get("/servers.json.hash")]
async fn hash(
    server_state: &State<ServerCache>,
    send_channel: &State<tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>>
) -> status::Accepted<String>  {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    send_channel.send(tx).expect("Failed to send request message");
    rx.await.expect("Failed to receive response message");
    let server_state = server_state.cache.lock().unwrap();
    status::Accepted(server_state.digest.clone())
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let args = Args::parse();
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(if args.debug { Level::TRACE } else { Level::INFO })
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default tracing subscriber failed");

    let servers_url: Url = args
        .url
        .as_ref()
        .map_or(DEFAULT_SERVERS_URL.try_into().expect("Failed to parse default server URL (bug)"), |u| u.clone());
    info!("Starting with upstream URL: {}", servers_url);
    let my_servers_file = std::fs::File::open(&args.servers).expect("Failed to open servers file");
    let my_servers = serde_json::from_reader(my_servers_file).expect("Failed to parse servers file");
    info!("Our server list loaded");
    let servers_upstream = reqwest::get(servers_url.clone()).await.expect("Failed to fetch initial server list")
        .json::<Servers>().await.expect("Failed to parse initial server list");
    info!("Upstream servers loaded");
    let cache_url = args.base_url.join("servers.json.hash").expect("Failed to create hash url from base url");
    let (mut merged_servers, digest) = merge_servers(&servers_upstream, &my_servers);
    merged_servers.cache = Some(cache_url.to_string());
    let server_cache = ServerCacheGated {
        merged_servers,
        my_servers,
        digest,
        last_update: Instant::now()
    };
    let server_state = ServerCache {
        cache: Arc::new(Mutex::new(server_cache))
    };

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
    let server_state_ref = server_state.cache.clone();
    tokio::spawn(async move {
        loop {
            let tx = rx.recv().await.expect("Failed to receive request message");
            let last_update = server_state_ref.lock().unwrap().last_update;
            let now = Instant::now();
            let delta = now - last_update;
            if delta > Duration::from_secs(60) {
                info!("Updating upstream servers (delta = {}s)", delta.as_secs());
                let servers_upstream_result = reqwest::get(servers_url.clone()).await;
                match servers_upstream_result {
                    Ok(response) => {
                        let response_json = response.json::<Servers>().await;
                        match response_json {
                            Ok(servers) => {
                                let mut server_state = server_state_ref.lock().unwrap();
                                (server_state.merged_servers, server_state.digest) = merge_servers(&servers, &server_state.my_servers);
                                server_state.merged_servers.cache = Some(cache_url.to_string());
                                server_state.last_update = now;
                            },
                            Err(e) => {
                                error!("Failed to parse servers: {}", e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("Failed to fetch servers: {}", e);
                    }
                }
            }
            tx.send(()).expect("Failed to send response message");
        }
    });

    let _rocket = rocket::build()
        .mount("/", routes![servers, hash])
        .manage(server_state)
        .manage(tx)
        .launch()
        .await?;

    Ok(())
}