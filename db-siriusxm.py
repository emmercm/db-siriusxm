#!/usr/bin/env python3

import argparse
from bs4 import BeautifulSoup
import pynumparser
import re
import requests
import sqlite3 as sql
import sys
import time
import xml.etree.ElementTree as et

SCRIPT_NAME = 'db-siriusxm'

REQUESTS_TIMEOUT = 5
SLEEP_MIN = 15
SLEEP_INCREMENT = 5
SLEEP_MAX = 60


parser = argparse.ArgumentParser(prog=SCRIPT_NAME)
parser.add_argument('-c', metavar='N[-N][,N-N]', dest='whitelist', help='channel whitelist', type=pynumparser.NumberSequence(limits=(1, None)))
parser.add_argument('-C', metavar='N[-N][,N-N]', dest='blacklist', help='channel blacklist', type=pynumparser.NumberSequence(limits=(1, None)))
args = parser.parse_args()
args.whitelist = list(args.whitelist or [])
args.blacklist = list(args.blacklist or [])


def log(s):
    # Insert timestamp before first non-special ASCII character
    s = str(s)
    for i, c in enumerate(s):
        if ord(c) >= 32:
            s = s[:i] + '[' + time.strftime('%H:%M:%S') + '] ' + s[i:]
            break
    sys.stdout.write(s)


# Create database tables, indexes
def db_create(db_curs):
    # sqlite3 was added in Python v2.5 (2006-09-19)
    # FOREIGN KEY support was added in sqlite v3.6.19 (2009-10-14)
    # So instead of REFERENCES() for FOREIGN KEYs we use:
    #   BEFORE INSERT (RESTRICT), AFTER UDPATE (CASCADE), BEFORE DELETE (CASCADE)

    # Try to use some HDD speedups
    db_curs.execute("PRAGMA journal_mode = MEMORY")

    # TABLE channels to store channel number/name
    db_curs.execute("""CREATE TABLE IF NOT EXISTS channels (
        number  INTEGER PRIMARY KEY,
        name    TEXT UNIQUE NOT NULL
        );""")

    # TABLE artists to store artist names
    db_curs.execute("""CREATE TABLE IF NOT EXISTS artists (
        _id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT UNIQUE NOT NULL
        );""")

    # TABLE tracks to store artist/title pairs
    db_curs.execute("""CREATE TABLE IF NOT EXISTS tracks (
        _id     INTEGER PRIMARY KEY AUTOINCREMENT,
        artist  INTEGER NOT NULL,
        title   TEXT NOT NULL
        );""")
    db_curs.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ix_tracks_artist_title ON tracks (artist, title)""")
    # tracks.artist REFERENCES artists(_id)  ON INSERT RESTRICT  ON UPDATE CASCADE  ON DELETE CASCADE
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_tracks_insert
        BEFORE INSERT on tracks
        FOR EACH ROW BEGIN
            SELECT CASE WHEN ((SELECT _id FROM artists WHERE _id = NEW.artist) IS NULL)
            THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_artists_update_id_tracks
        AFTER UPDATE OF _id ON artists
        FOR EACH ROW BEGIN
            UPDATE tracks SET artist = NEW._id WHERE channel = OLD._id;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_artists_delete_tracks
        BEFORE DELETE ON artists
        FOR EACH ROW BEGIN
            DELETE FROM tracks WHERE artist = OLD._id;
        END;""")

    # TABLE entries to store channel/track/time records
    db_curs.execute("""CREATE TABLE IF NOT EXISTS entries (
        _id      INTEGER PRIMARY KEY AUTOINCREMENT,
        channel  INTEGER NOT NULL,
        track    INTEGER NOT NULL,
        time     INTEGER NOT NULL
        );""")
    db_curs.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ix_entries_channel_time ON entries (channel, time)""")
    # entries.channel REFERENCES channels(number)  ON INSERT RESTRICT  ON UPDATE CASCADE  ON DELETE CASCADE
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_entries_insert_channel
        BEFORE INSERT on entries
        FOR EACH ROW BEGIN
            SELECT CASE WHEN ((SELECT number FROM channels WHERE number = NEW.channel) IS NULL)
            THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_channels_update_number_entries
        AFTER UPDATE OF number ON channels
        FOR EACH ROW BEGIN
            UPDATE entries SET channel = NEW.number WHERE channel = OLD.number;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_channels_delete_entries
        BEFORE DELETE ON channels
        FOR EACH ROW BEGIN
            DELETE FROM entries WHERE channel = OLD.number;
        END;""")
    # entries.track REFERENCES tracks(_id)  ON INSERT RESTRICT  ON UPDATE CASCADE  ON DELETE CASCADE
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_entries_insert_tracks
        BEFORE INSERT on entries
        FOR EACH ROW BEGIN
            SELECT CASE WHEN ((SELECT _id FROM tracks WHERE _id = NEW.track) IS NULL)
            THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_tracks_update_id_entries
        AFTER UPDATE OF _id ON tracks
        FOR EACH ROW BEGIN
            UPDATE entries SET track = NEW._id WHERE channel = OLD._id;
        END;""")
    db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_tracks_delete_entries
        BEFORE DELETE ON tracks
        FOR EACH ROW BEGIN
            DELETE FROM entries WHERE track = OLD._id;
        END;""")


def db_insert(db_curs, channels):
    entries_inserted = 0
    if len(channels) > 0:
        db_curs.execute("BEGIN")

    # Get applicable channel names
    db_curs.execute("""SELECT number, name FROM channels
                    WHERE number IN ({})""".format(','.join('?' * len(channels))),
                    [channel['channel'] for channel in channels])
    channel_names = db_curs.fetchall()
    channel_names = {channel[0]: channel[1] for channel in channel_names}

    # Get applicable artist IDs
    db_curs.execute("""SELECT name, _id FROM artists
                    WHERE name IN ({})""".format(','.join('?' * len(channels))),
                    [channel['artist'] for channel in channels])
    artists = db_curs.fetchall()
    artists = {artist[0]: artist[1] for artist in artists}

    for channel in channels:
        channel_id = channel['channel']
        channel_name = channel_names[channel_id] if channel_id in channel_names else None
        if channel_name is None or channel_name != channel['name']:
            db_curs.execute("""INSERT OR REPLACE INTO channels (number, name) VALUES (?, ?)""",
                            (channel_id, channel['name']))
            channel_name = channel['name']

        artist_id = artists[channel['artist']] if channel['artist'] in artists else None
        if artist_id is None:
            db_curs.execute("""INSERT INTO artists (name) VALUES (?)""", (channel['artist'],))
            db_curs.execute("""SELECT _id FROM artists WHERE name = ?""", (channel['artist'],))
            artist_id = db_curs.fetchone()[0]  # assumes INSERT is good
            artists[channel['artist']] = artist_id

        db_curs.execute("""SELECT _id FROM tracks WHERE artist = ? AND title = ?""", (artist_id, channel['track']))
        track_id = db_curs.fetchone()
        if track_id is None:
            db_curs.execute("""INSERT INTO tracks (artist, title) VALUES (?, ?)""", (artist_id, channel['track']))
            db_curs.execute("""SELECT _id FROM tracks WHERE artist = ? AND title = ?""", (artist_id, channel['track']))
            track_id = db_curs.fetchone()[0]  # assumes INSERT is good
        else:
            track_id = track_id[0]

        # Check if we already have the same entry recently
        db_curs.execute(
            """SELECT track FROM entries WHERE channel = ? AND track = ?
            AND datetime(time,'unixepoch') > datetime('now', '-10 minutes')""",
            (channel_id, track_id))
        entry_recent = db_curs.fetchone()
        if entry_recent is not None:
            continue  # current track was already recorded
        try:
            db_curs.execute("""INSERT INTO entries (channel, track, time) VALUES (?, ? ,?)""",
                            (channel_id, track_id, channel['time']))
        except sql.IntegrityError:  # sqlite3 throws FOREIGN KEY CONSTRAINT incorrectly sometimes?
            pass
        else:
            entries_inserted += 1

    if len(channels) > 0:
        db_curs.execute("COMMIT")
    return entries_inserted


def db_count(db_curs):
    db_curs.execute("""SELECT COUNT(*) FROM entries""")
    return db_curs.fetchall()[0][0]


# Given a URL return an ElementTree root
def html_get_tree(url):
    try:
        resp = requests.get(url, timeout=REQUESTS_TIMEOUT)
        resp.encoding = 'utf-8'
        html = resp.text
    except:
        return et.fromstring('<html></html>')

    # Strip unnecessary trouble elements
    html = re.sub(r'<!--.*?-->>', '', html, flags=re.DOTALL)
    html = re.sub(r'<![^>]+>', '', html, flags=re.DOTALL)
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    html = re.sub(r'<html [^>]+>', '<html>', html, flags=re.DOTALL)
    # Remove newlines in tags
    for tag in re.findall('(<[^>]+>)', html):
        tag_repl = tag
        tag_repl = str.replace(tag_repl, '\r', '')
        tag_repl = str.replace(tag_repl, '\n', '')
        html = str.replace(html, tag, tag_repl)

    # Parse garbage HTML with BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    # Parse cleaned HTML with ElementTree
    root = et.fromstring(str(soup))
    return root


# Scrape dogstarradio.com
def scrape_dogstar_radio():
    channels = []

    root = html_get_tree('http://www.dogstarradio.com/now_playing.php')
    for td in root.findall('.//table//td[div]'):
        channel = {'channel': 0, 'name': '', 'artist': '', 'track': '', 'time': int(time.time())}

        td_text = td.text
        td_text = re.sub(r'[\r\n]', '', td_text)

        # Get channel number
        chan_num = re.findall('^([0-9]+)', td_text)
        if len(chan_num) == 0:
            continue
        channel['channel'] = int(chan_num[0])

        # Get channel name
        for a in td.findall('.//a'):
            a_text = a.text
            a_text = re.sub(r'[\r\n]', '', a_text)
            channel['name'] = a_text

        # Get channel artist and track
        for div in td.findall('.//div'):
            div_text = div.text
            div_text = re.sub(r'[\r\n]', '', div_text)
            div_split = div_text.split(' - ', 1)
            if len(div_split) == 2:
                channel['artist'] = div_split[0]
                channel['track'] = div_split[1]

        if channel['track'] == 'data by DogstarRadio.com':
            continue

        channels.append(channel)

    return channels


# XMFan.com does some stripping of artist/title info

# Clean the output from scrapers
def scrape_clean(channels):
    for channel in channels[:]:
        # Unset whitelist/blacklist channels
        if (len(args.whitelist) > 0 and not channel['channel'] in args.whitelist) or (
                    channel['channel'] in args.blacklist):
            channels.remove(channel)
            continue
        # Clean strings
        if channel['name'] == '':
            channel['name'] = 'Channel ' + str(channel['channel'])
        for key in channel.keys():
            if type(channel[key]) is str:
                channel[key] = channel[key].strip()
    return channels


# Connect to database
db_conn = sql.connect(SCRIPT_NAME + '.db')
db_conn.text_factory = str
db_conn.isolation_level = None
db_curs = db_conn.cursor()
db_create(db_curs)
log("Total entries: " + str(db_count(db_curs)) + "\n")
log("\n")

# Continually scrape and INSERT
sleep = SLEEP_MIN
sleep_time = time.time()
while True:
    try:
        # Scrape channels
        log("Scraping channels...")
        channels = []
        if len(channels) == 0:
            channels = scrape_dogstar_radio()
        # TODO: More scrapers
        channels = scrape_clean(channels)
        sleep_time = time.time()

        # Insert scraped data into database
        inserted = 0
        if len(channels) > 0:
            log("\rScraped " + str(len(channels)) + " channels, adding entries...")
            inserted = db_insert(db_curs, channels)
            sleep = SLEEP_MIN
        else:
            sleep += SLEEP_INCREMENT
            if sleep > SLEEP_MAX:
                sleep = SLEEP_MAX
        log("\rScraped " + str(len(channels)) + " channels, added " + str(inserted) + " entries\n")

        # Sleep some time to reduce scraping page impact
        sleep_adj = int(sleep - (time.time() - sleep_time))
        if sleep_adj > 0:
            log("Waiting " + str(sleep_adj) + "s...")
            time.sleep(sleep_adj)
            log("\r")

    except KeyboardInterrupt:
        break

# Close database
log("\n\n")
log("Total entries: " + str(db_count(db_curs)) + "\n")
db_conn.commit()
db_conn.close()
