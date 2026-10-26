import fs from "fs-extra";
import MemoryStore from "memory-chunk-store";
import os from "os";
import path from "path";
import WebTorrent, { Torrent } from "webtorrent";
import { getReadableDuration } from "../utils/file.js";

interface FileInfo {
  name: string;
  path: string;
  size: number;
  url?: string;
}

interface ActiveFileInfo extends FileInfo {
  progress: number;
  downloaded: number;
}

export interface TorrentInfo {
  name: string;
  infoHash: string;
  size: number;
  files: FileInfo[];
}

interface ActiveTorrentInfo extends TorrentInfo {
  progress: number;
  downloaded: number;
  uploaded: number;
  downloadSpeed: number;
  uploadSpeed: number;
  peers: number;
  openStreams: number;
  liveFileStreams: number;
  files: ActiveFileInfo[];
}

// Directory to store downloaded files (default OS temp directory)
const DOWNLOAD_DIR =
  process.env.DOWNLOAD_DIR || path.join(os.tmpdir(), "torrent-stream-server");

// Keep downloaded files after all streams are closed (default false)
const KEEP_DOWNLOADED_FILES = process.env.KEEP_DOWNLOADED_FILES
  ? process.env.KEEP_DOWNLOADED_FILES === "true"
  : false;

if (!KEEP_DOWNLOADED_FILES) fs.emptyDirSync(DOWNLOAD_DIR);

// Maximum number of connections per torrent (default 50)
const MAX_CONNS_PER_TORRENT = Number(process.env.MAX_CONNS_PER_TORRENT) || 50;

// Max download speed (bytes/s) over all torrents (default 20MB/s)
const DOWNLOAD_SPEED_LIMIT =
  Number(process.env.DOWNLOAD_SPEED_LIMIT) || 20 * 1024 * 1024;

// Max upload speed (bytes/s) over all torrents (default 1MB/s)
const UPLOAD_SPEED_LIMIT =
  Number(process.env.UPLOAD_SPEED_LIMIT) || 1 * 1024 * 1024;

// Time (ms) to seed torrents after all streams are closed (default 1 minute)
const SEED_TIME = Number(process.env.SEED_TIME) || 60 * 1000;

// Timeout (ms) when adding torrents if no metadata is received (default 5 seconds)
const TORRENT_TIMEOUT = Number(process.env.TORRENT_TIMEOUT) || 5 * 1000;

// In-memory piece cache slots per streamed torrent (default 5, webtorrent default 20)
const STORE_CACHE_SLOTS =
  process.env.STORE_CACHE_SLOTS !== undefined
    ? Number(process.env.STORE_CACHE_SLOTS)
    : 5;

// Max bytes queued for upload per peer before its new block requests are refused (default 2 MB)
const MAX_PEER_UPLOAD_QUEUE =
  Number(process.env.MAX_PEER_UPLOAD_QUEUE) || 2 * 1024 * 1024;

const infoClient = new WebTorrent();
const streamClient = new WebTorrent({
  // @ts-ignore
  downloadLimit: DOWNLOAD_SPEED_LIMIT,
  uploadLimit: UPLOAD_SPEED_LIMIT,
  maxConns: MAX_CONNS_PER_TORRENT,
});

streamClient.on("torrent", (torrent) => {
  console.log(`Added torrent: ${torrent.name}`);
});

streamClient.on("error", (error) => {
  if (typeof error === "string") {
    console.error(`Error: ${error}`);
  } else {
    if (error.message.startsWith("Cannot add duplicate torrent")) return;
    console.error(`Error: ${error.message}`);
  }
});

infoClient.on("error", () => {});

// bittorrent-protocol answers every block request and pushes the data without
// backpressure, while UPLOAD_SPEED_LIMIT throttles it downstream. Peers request far
// faster than we upload, so answered blocks piled up in RAM (each one could pin a
// whole 4-16 MB piece). Refuse new requests while a peer's queue is full.
const limitUploadQueue = (wire: any) => {
  const onRequest = wire._onRequest;
  if (typeof onRequest !== "function") return;

  wire._onRequest = function (index: number, offset: number, length: number) {
    const queued = this.readableLength + this.peerRequests.length * length;
    if (queued >= MAX_PEER_UPLOAD_QUEUE) {
      if (this.hasFast) this.reject(index, offset, length);
      return;
    }
    return onRequest.call(this, index, offset, length);
  };
};

const launchTime = Date.now();

export const getStats = () => ({
  uptime: getReadableDuration(Date.now() - launchTime),
  memory: process.memoryUsage(),
  pendingTimeouts: timeouts.size,
  openStreams: [...openStreams.values()].reduce((a, b) => a + b, 0),
  downloadSpeed: streamClient.downloadSpeed,
  uploadSpeed: streamClient.uploadSpeed,
  activeTorrents: streamClient.torrents.map<ActiveTorrentInfo>((torrent) => ({
    name: torrent.name,
    infoHash: torrent.infoHash,
    size: torrent.length,
    progress: torrent.progress,
    downloaded: torrent.downloaded,
    uploaded: torrent.uploaded,
    downloadSpeed: torrent.downloadSpeed,
    uploadSpeed: torrent.uploadSpeed,
    peers: torrent.numPeers,
    openStreams: openStreams.get(torrent.infoHash) || 0,
    liveFileStreams: torrent.files.reduce(
      // @ts-ignore
      (sum, file) => sum + (file._fileStreams?.size || 0),
      0
    ),
    files: torrent.files.map((file) => ({
      name: file.name,
      path: file.path,
      size: file.length,
      progress: file.progress,
      downloaded: file.downloaded,
    })),
  })),
});

export const getOrAddTorrent = (uri: string) =>
  new Promise<Torrent | undefined>((resolve) => {
    const torrent = streamClient.add(
      uri,
      {
        path: DOWNLOAD_DIR,
        destroyStoreOnDestroy: !KEEP_DOWNLOADED_FILES,
        // @ts-ignore
        storeCacheSlots: STORE_CACHE_SLOTS,
        // @ts-ignore
        deselect: true,
      },
      (torrent) => {
        clearTimeout(timeout);
        resolve(torrent);
      }
    );

    // Wires connect before metadata arrives, so hook them here rather than on "ready"
    torrent.on("wire", limitUploadQueue);

    const timeout = setTimeout(() => {
      torrent.destroy();
      resolve(undefined);
    }, TORRENT_TIMEOUT);
  });

export const getFile = (torrent: Torrent, path: string) =>
  torrent.files.find((file) => file.path === path);

export const getTorrentInfo = async (uri: string) => {
  const getInfo = (torrent: Torrent): TorrentInfo => ({
    name: torrent.name,
    infoHash: torrent.infoHash,
    size: torrent.length,
    files: torrent.files.map((file) => ({
      name: file.name,
      path: file.path,
      size: file.length,
    })),
  });

  return await new Promise<TorrentInfo | undefined>((resolve) => {
    const torrent = infoClient.add(
      uri,
      { store: MemoryStore, destroyStoreOnDestroy: true },
      (torrent) => {
        clearTimeout(timeout);
        const info = getInfo(torrent);
        console.log(`Fetched info: ${info.name}`);
        torrent.destroy();
        resolve(info);
      }
    );

    const timeout = setTimeout(() => {
      torrent.destroy();
      resolve(undefined);
    }, TORRENT_TIMEOUT);
  });
};

const timeouts = new Map<string, NodeJS.Timeout>();
const openStreams = new Map<string, number>();

export const streamOpened = (hash: string, fileName: string) => {
  console.log(`Stream opened: ${fileName}`);

  const count = openStreams.get(hash) || 0;
  openStreams.set(hash, count + 1);

  const timeout = timeouts.get(hash);

  if (timeout) {
    clearTimeout(timeout);
    timeouts.delete(hash);
  }
};

export const streamClosed = (hash: string, fileName: string) => {
  console.log(`Stream closed: ${fileName}`);

  const count = openStreams.get(hash) || 1;
  openStreams.set(hash, count - 1);

  if (count > 1) return;

  openStreams.delete(hash);

  let timeout = timeouts.get(hash);
  if (timeout) return;

  timeout = setTimeout(async () => {
    // Always clear the entry, a stale one blocks future removal of this torrent
    timeouts.delete(hash);
    if (openStreams.get(hash)) return;

    const torrent = await streamClient.get(hash);
    if (!torrent) return;

    // @ts-ignore
    torrent.destroy(undefined, () => {
      console.log(`Removed torrent: ${torrent.name}`);
    });
  }, SEED_TIME);

  timeouts.set(hash, timeout);
};
