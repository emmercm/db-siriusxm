from bs4 import BeautifulSoup
import re
import requests
import sqlite3 as sql
import string
import sys
import time
import xml.etree.ElementTree as et

SCRIPT_NAME = 'db-siriusxm'

REQUESTS_TIMEOUT = 5
SLEEP_MIN = 15
SLEEP_INCREMENT = 5
SLEEP_MAX = 60


def log(s):
	# Insert timestamp before first non-special ASCII character
	s = str(s)
	for i, c in enumerate(s):
		if ord(c) >= 32:
			s = s[:i] + '[' + time.strftime('%H:%M:%S') + '] ' + s[i:]
			break
	sys.stdout.write(s)
	

# Create database tables, indexes
def DB_CREATE(db_curs):
	# sqlite3 was added in Python v2.5 (2006-09-19)
	# FOREIGN KEY support was added in sqlite v3.6.19 (2009-10-14)
	# So instead of REFERENCES() for FOREIGN KEYs we use: BEFORE INSERT (RESTRICT), AFTER UDPATE (CASCADE), BEFORE DELETE (CASCADE)
	
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
			SELECT CASE WHEN ((SELECT _id FROM artists WHERE _id = NEW.artist) IS NULL) THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
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
	# entries.channel REFERENCES channels(number)  ON INSERT RESTRICT  ON UPDATE CASCADE  ON DELETE CASCADE
	db_curs.execute("""CREATE TRIGGER IF NOT EXISTS tr_entries_insert_channel
		BEFORE INSERT on entries
		FOR EACH ROW BEGIN
			SELECT CASE WHEN ((SELECT number FROM channels WHERE number = NEW.channel) IS NULL) THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
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
			SELECT CASE WHEN ((SELECT _id FROM tracks WHERE _id = NEW.track) IS NULL) THEN RAISE(ABORT, 'FOREIGN KEY CONSTRAINT') END;
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

def DB_INSERT(db_curs, channels):
	entries_inserted = 0
	if len(channels) > 0: db_curs.execute("BEGIN")
	
	for channel in channels:
		db_curs.execute("""SELECT name FROM channels WHERE number = ?""", (channel['channel'],))
		channel_name = db_curs.fetchone()
		if channel_name is None or channel_name[0] != channel['name']:
			db_curs.execute("""INSERT OR REPLACE INTO channels (number, name) VALUES (?, ?)""", (channel['channel'], channel['name']))
		channel_id = channel['channel']
		
		db_curs.execute("""SELECT _id FROM artists WHERE name = ?""", (channel['artist'],))
		artist_id = db_curs.fetchone()
		if artist_id is None:
			db_curs.execute("""INSERT INTO artists (name) VALUES (?)""", (channel['artist'],))
			db_curs.execute("""SELECT _id FROM artists WHERE name = ?""", (channel['artist'],))
			artist_id = db_curs.fetchone()[0]  # assumes INSERT is good
		else:
			artist_id = artist_id[0]
			
		db_curs.execute("""SELECT _id FROM tracks WHERE artist = ? AND title = ?""", (artist_id, channel['track']))
		track_id = db_curs.fetchone()
		if track_id is None:
			db_curs.execute("""INSERT INTO tracks (artist, title) VALUES (?, ?)""", (artist_id, channel['track']))
			db_curs.execute("""SELECT _id FROM tracks WHERE artist = ? AND title = ?""", (artist_id, channel['track']))
			track_id = db_curs.fetchone()[0]  # assumes INSERT is good
		else:
			track_id = track_id[0]
			
		# Check if we already have the same entry recently
		db_curs.execute("""SELECT track FROM entries WHERE channel = ? AND track = ? AND datetime(time,'unixepoch') > datetime('now','-10 minutes')""", (channel_id, track_id))
		entry_recent = db_curs.fetchone()
		if not entry_recent is None: continue  # current track was already recorded
		db_curs.execute("""INSERT INTO entries (channel, track, time) VALUES (?, ? ,?)""", (channel_id, track_id, channel['time']))
		entries_inserted +=1
		
	if len(channels) > 0: db_curs.execute("COMMIT")
	return entries_inserted

def DB_COUNT(db_curs):
	db_curs.execute("""SELECT COUNT(*) FROM entries""")
	return db_curs.fetchall()[0][0]


# Given a URL return an ElementTree root
def HTML_GetTree(url):
	try:
		resp = requests.get(url, timeout=REQUESTS_TIMEOUT)
		html = resp.text.encode('utf-8')
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
		tag_repl = string.replace(tag_repl, '\r', '')
		tag_repl = string.replace(tag_repl, '\n', '')
		html = string.replace(html, tag, tag_repl)
		
	# Parse garbage HTML with BeautifulSoup
	soup = BeautifulSoup(html)
	# Parse cleaned HTML with ElementTree
	root = et.fromstring(str(soup))
	return root

# Scrape dogstarradio.com
def Scrape_DogstarRadio():
	channels = []
	
	root = HTML_GetTree('http://www.dogstarradio.com/now_playing.php')
	for td in root.findall('.//table//td[div]'):
		channel = {'channel':0, 'name':'', 'artist':'', 'track':'', 'time':int(time.time())}
		
		td_text = td.text.encode('utf-8')
		td_text = re.sub(r'[\r\n]', '', td_text)
		
		# Get channel number
		chan_num = re.findall('^([0-9]+)', td_text)
		if len(chan_num) == 0: continue
		channel['channel'] = int(chan_num[0])
		
		# Get channel name
		for a in td.findall('.//a'):
			a_text = a.text.encode('utf-8')
			a_text = re.sub(r'[\r\n]', '', a_text)
			channel['name'] = a_text
			
		# Get channel artist and track
		for div in td.findall('.//div'):
			div_text = div.text.encode('utf-8')
			div_text = re.sub(r'[\r\n]', '', div_text)
			div_split = div_text.split(' - ', 1)
			if len(div_split) == 2:
				channel['artist'] = div_split[0]
				channel['track'] = div_split[1]
				
		if channel['track'] == 'data by DogstarRadio.com': continue
		
		channels.append(channel)
		# if channel['channel'] < 100: print channel
		
	return channels
	
# XMFan.com does some stripping of artist/title info
	
# Clean the output from scrapers
def Scrape_Clean(channels):
	for (idx, channel) in enumerate(channels):
		for key in channel.keys():
			if type(channel[key]) is str:
				channels[idx][key] = channel[key].strip()
	return channels


# Connect to database
db_conn = sql.connect(SCRIPT_NAME + '.db')
db_conn.text_factory = str
db_conn.isolation_level = None
db_curs = db_conn.cursor()
DB_CREATE(db_curs)
log("Total entries: " + str(DB_COUNT(db_curs)) + "\n")
log("\n")

# Continually scrape and INSERT
sleep = SLEEP_MIN
sleep_time = time.time()
while True:
	try:
		# Scrape channels
		log("Scraping channels...")
		channels = []
		if len(channels) == 0: channels = Scrape_DogstarRadio()
		# TODO: More scrapers
		channels = Scrape_Clean(channels)
		sleep_time = time.time()
		
		# Insert scraped data into database
		inserted = 0
		if len(channels) > 0:
			log("\rScraped " + str(len(channels)) + " channels, adding entries...")
			inserted = DB_INSERT(db_curs, channels)
			sleep = SLEEP_MIN
		else:
			sleep += SLEEP_INCREMENT
			if sleep > SLEEP_MAX: sleep = SLEEP_MAX
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
log("Total entries: " + str(DB_COUNT(db_curs)) + "\n")
db_conn.commit()
db_conn.close()