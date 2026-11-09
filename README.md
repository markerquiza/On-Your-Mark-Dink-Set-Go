# On Your Mark, Dink, Set, Go!

A standalone pickleball court desk for running doubles open play on your own computer. It stores player profiles, queues, sessions, and scores in local SQLite. It works offline after Python is installed.

## Start the desk

Install Python 3.9 or newer from [python.org](https://www.python.org/downloads/), then run this from the project folder:

```sh
python3 server.py
```

On Windows, use `py server.py` if `python3` is unavailable. Open <http://127.0.0.1:8765> in your browser. Press Ctrl+C in the terminal to stop the desk.

A new database starts with eight courts. To create a separate desk with four courts, run `python3 server.py --db my-desk.sqlite3 --courts 4`.

Use **Open play** to manage sessions, **Court desk** to register players and run matches, **Players** to manage profiles, and **History** to review scores and rosters. Finish a match to return its players to the queue automatically. Turn off automatic assignment if you want to call groups manually.

The desk is available only on the computer running it. Keep the SQLite file and its backups private. Use **Back up now** for a verified snapshot, and keep a copy on a separate drive.

## Run the tests

```sh
python3 -B -m unittest discover -s tests -q
```
