import argparse, os, sys, pathlib, re, csv, requests, warnings, time
from pprint import pprint
from rdflib import Graph, Literal, RDF, URIRef, BNode
from rdflib.namespace import Namespace, DCTERMS, FOAF, PROV, RDFS, XSD
from urllib.parse import quote
from fuzzywuzzy import fuzz


# Music Ontology namespaces
MO = Namespace("http://purl.org/ontology/mo/")
TL = Namespace("http://purl.org/NET/c4dm/timeline.owl#")
EV = Namespace("https://purl.org/NET/c4dm/event.owl#")
#MusicBrainz namespaces
ARTIST = Namespace("https://musicbrainz.org/artist/")
WORK = Namespace("https://musicbrainz.org/work/")
RELEASE = Namespace("https://musicbrainz.org/work/")
ISRC = Namespace("https://musicbrainz.org/isrc/")
RECORDING = Namespace("https://musicbrainz.org/recording/")
TRACK = Namespace("https://musicbrainz.org/track/")
SSVRelease = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/release/")
SSVReleaseEvent = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/release-event/")
SSVSignal = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/signal/")
SSVRecord = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/record/")
SSVTrack = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/track/")
SSVPerformance = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/performance/")
SSVPerformer = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/rdf/performer/")
SSVO = Namespace("https://repo.mdw.ac.at/signature-sound-vienna/ontology/ssv/")

def parse_cue_file(file_path, debug):
    with open(file_path) as file:
        lines = file.readlines()
        parsed = {}
        parsed["file_path"] = file_path
        parsed["header"] = {}
        current_track = None
        for line in lines:
            line = line.strip()
            if current_track is None:
                print(line)
                mbz_header_artist_match = re.compile ('REM MUSICBRAINZ_ALBUM_ARTIST_ID (.*)').match(line)
                mbz_album_match = re.compile ('REM MUSICBRAINZ_ALBUM_ID (.*)').match(line)
                header_match = re.compile('REM *(.*) (.*)').match(line)
                cat_match = re.compile("CATALOG (.*$)").match(line)
                title_match = re.compile("TITLE (.*$)").match(line)
                perf_match = re.compile("PERFORMER (.*$)").match(line)
                track_match = re.compile(" *TRACK (\d+) AUDIO").match(line)
                if mbz_header_artist_match:
                    # n.b. can be multiple IDs separated by semi-colons
                    parsed["header"]["mbz_artist_list"] = mbz_header_artist_match[1].split(";")
                elif mbz_album_match:
                    parsed["header"]["mbz_album_id"] = mbz_album_match[1]
                elif header_match:
                    parsed["header"][header_match[1].lower()] = header_match[2]
                elif cat_match:
                    parsed["header"]["catalog"] = cat_match[1]
                elif title_match:
                    parsed["header"]["title"] = title_match[1]
                elif perf_match:
                    parsed["header"]["performer"] = perf_match[1]
                elif track_match:
                    current_track = int(track_match[1])
                    parsed[current_track] = {}
                elif debug: 
                    print("skipping line: ", line)
            else:
                mbz_track_match = re.compile(" *REM MUSICBRAINZ_TRACK_ID (.*$)").match(line)
                mbz_artist_match = re.compile(" *REM MUSICBRAINZ_ARTIST_ID (.*$)").match(line)
                title_match = re.compile(" *TITLE (.*$)").match(line)
                perf_match = re.compile(" *PERFORMER (.*$)").match(line)
                isrc_match = re.compile(" *ISRC (.*$)").match(line)
                pregap_match = re.compile(" *PREGAP (.*$)").match(line)
                index_match = re.compile(" *INDEX 01 (.*$)").match(line)
                track_match = re.compile(" *TRACK (\d+) AUDIO").match(line)
                if title_match:
                    parsed[current_track]["title"] = title_match[1]
                elif mbz_track_match:
                    parsed[current_track]["mbz_track"] = mbz_track_match[1]
                elif mbz_artist_match:
                    parsed[current_track]["mbz_artist"] = mbz_artist_match[1]
                elif perf_match:
                    parsed[current_track]["performer"] = perf_match[1]
                elif isrc_match:
                    parsed[current_track]["isrc"] = isrc_match[1]
                elif pregap_match:
                    parsed[current_track]["pregap"] = pregap_match[1]
                elif index_match:
                    parsed[current_track]["index"] = index_match[1]
                elif track_match:
                    current_track = int(track_match[1])
                    parsed[current_track] = {}
                elif debug: 
                    print("skipping line: ", line)
    return parsed

def write_rdf(parsed, rdf_file, path):
    g = Graph()
    for p in parsed:
        # build a URI component to be used in the various URIs we generate for this release / record
        ssvUriComponent = quote(p['file_path'].parent.as_posix()).replace(quote(path).rstrip("/"), "").lstrip("/")
        release = URIRef(SSVRelease + str(ssvUriComponent))
        release_event = URIRef(SSVReleaseEvent + str(ssvUriComponent))
        record = URIRef(SSVRecord + str(ssvUriComponent))
        mbz_album_json = None
        if 'mbz_album_id' in p['header']:
            # if we have musicbrainz identifiers, request them from mbz...
            time.sleep(0.3) # be polite
            try:
                r = requests.get("https://musicbrainz.org/album/" + p['header']['mbz_album_id'], headers={"Accept": "application/ld+json"})
                r.raise_for_status()
                print("Response:")
                pprint(r.text)
                mbz_album_json = r.json()
            except requests.exceptions.HTTPError as err:
                warnings.warn("Could not GET Musicbrainz album "+ p['header']['mbz_album_id'] + ": " + err)
        if mbz_album_json: 
            pprint(mbz_album_json)

        #--------------RELEASE--------------#
        g.add((release, RDF.type, MO.Release))
        g.add((release, DCTERMS.title, Literal(p['header'].get('title', '__NONE__'))))
        g.add((release, RDFS.label, Literal("Release: " + p['header'].get('title', '__NONE__'))))
        #g.add((SSVRelease, MO.catalogue_number, p['header'].get('cddbcat', '__NONE__'))
        g.add((release, MO.catalogue_number, Literal(p['header'].get('catalogue_number', '__NONE__'))))
        g.add((release, MO.record, record))
        
        #-----------RELEASE EVENT----------#
        g.add((release_event, RDF.type, MO.ReleaseEvent))
        g.add((release_event, RDF.type, EV.Event))
        g.add((release_event, MO.release, release))
        release_event_time = BNode()
        g.add((release_event, EV.time, release_event_time))
        g.add((release_event_time, RDF.type, TL.Instant))
        g.add((release_event_time, TL.atYear, Literal(p['header'].get('date', '__NONE__'), datatype=XSD.gYear)))

        #--------------RECORD--------------#
        g.add((record, RDF.type, MO.Record))
        g.add((release, RDFS.label, Literal("Record: " + p['header'].get('title', '__NONE__'))))
        g.add((record, MO.track_count, Literal(len(p)-1)))
        if 'musicbrainz_album_id' in p['header']:
            g.add((record, MO.musicbrainz, RELEASE.p['header']['musicbrainz_album_id']))
        for track_num in p:
            if track_num == 'header' or track_num == 'file_path':
                continue
            tix = str(ssvUriComponent) + '-' + str(track_num)
            track = URIRef(SSVTrack + tix)
            signal = URIRef(SSVSignal + tix)
            performance = URIRef(SSVPerformance + tix)
            performer = URIRef(SSVPerformer + tix)

            g.add((record, MO.track, track))
            g.add((release, MO.publication_of, signal))
            #--------------SIGNAL--------------#
            g.add((signal, RDF.type, MO.Signal))
            g.add((signal, MO.published_as, track))
            if 'isrc' in p[track_num]:
                isrc = p[track_num]['isrc']
                g.add((signal, MO.isrc, URIRef(ISRC + isrc)))
            #--------------TRACK--------------#
            g.add((track, RDF.type, MO.Track))
            if 'mbz_track' in p[track_num]:
                mbz_track_id = str(p[track_num]['mbz_track'])
                g.add((track, MO.musicbrainz, URIRef(TRACK + mbz_track_id)))
            g.add((track, MO.track_number, Literal(int(track_num))))
            g.add((track, RDFS.label, Literal("Track: " + p[track_num]["title"])))
            #--------------WORK----------------#
            # We can only leap to an authoritative (MusicBrainz) work if:
            # 1. We have a MBz album ID and have received data for it from the API
            # 2. The corresponding track entry has a work associated with it on MBz
            work = None
            if mbz_album_json:
                # First, locate the current track in the album data
                mbz_tracks_json = mbz_album_json['track']
                try: 
                    # mbz has track numbers like 1.13 (13th track on disc 1)
                    # filter out just the track num itself and compare it to our p track_num
                    print("Looking for track num: ", str(track_num))
                    mbz_track_json = [t for t in mbz_tracks_json if t['trackNumber'][t['trackNumber'].index(".")+1:] == str(track_num)]
                except ValueError:
                    sys.exit("Unexpected trackNumber format: {}".format(t['trackNumber']))
                # if we have more than one match (e.g., because multiple discs) try to disambiguate with title similarity
                if len(mbz_track_json) > 1:
                    similarities = [fuzz.ratio(t['name'], p[track_num]['title']) for t in mbz_track_json]
                    close_match_indices = [ix for ix, val in enumerate(similarities) if val > 90]
                    mbz_track_json = [mbz_track_json[i] for i in close_match_indices]
                # if we still have more than one match, warn the user (and default to first close-similarity match)
                if len(mbz_track_json) > 1:
                    warnings.warn("Multiple matches on track disambiguation, please sort manually: {} ##### {}".format(mbz_track_json, p[track_num]))
                if len(mbz_track_json) == 0:
                    warnings.warn("Can't find unique matching cue track name {cueName}".format(cueName=p[track_num]["title"]))
                else: 
                    if 'recordingOf' in mbz_track_json[0]:
                        rec = mbz_track_json[0]['recordingOf']
                        if isinstance(rec, list):
                            # associated with multiple works - suspicious...
                            warnings.warn("Track associated with multiple works: " + mbz_track_json[0]["@id"])
                        else:
                            rec = [rec]
                        for r in rec:
                            work = URIRef(r['@id'])
                            g.add((work, RDF.type, MO.MusicalWork))
                            g.add((work, DCTERMS.title, Literal(r['name'])))
                            g.add((work, RDFS.label, Literal("Work: " + r['name'])))
                    else:
                        warnings.warn("No work associated with MBz track: {}".format(mbz_track_json[0]["@id"]))

            #--------------PERFORMANCE--------------#
            g.add((performance, RDF.type, MO.Performance))
            g.add((performance, MO.recorded_as, signal))
            if work:
                g.add((performance, MO.performance_of, work))
            g.add((performance, RDFS.label, Literal("Performance: " + p[track_num]["title"])))
            #--------------PERFORMER--------------#
            g.add((performer, RDF.type, MO.MusicArtist))
            g.add((performer, MO.performed, performance))
            g.add((performer, FOAF.name, Literal(p[track_num]["performer"])))
            g.add((performer, RDFS.label, Literal("Performer: " + p[track_num]["performer"])))
            if 'mbz_artist' in p[track_num]:
                mbz_artist_ids = p[track_num]['mbz_artist'].split("; ") # in case of multiple artists
                for mbz_artist_id in mbz_artist_ids:
                    g.add((performer, MO.musicbrainz, URIRef(ARTIST + mbz_artist_id.replace('"', '') )))
    g.serialize(destination=rdf_file, format="text/turtle")

def write_headers_csv(parsed, headers_csv_file):
    with open(headers_csv_file, 'w', newline='') as csvfile:
        fieldnames = ['ix', 'title', 'performer', 'genre', 'catalog', 'cddbcat', 'comment', 'date', 'discid', 'volid']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for ix, p in enumerate(parsed):
            writer.writerow({
                'ix':       ix,
                'title':    p['header'].get('title', '__NONE__'),
                'performer':p['header'].get('performer', '__NONE__'),
                'genre':    p['header'].get('genre', '__NONE__'),
                'catalog':  p['header'].get('catalog', '__NONE__'),
                'cddbcat':  p['header'].get('cddbcat', '__NONE__'),
                'comment':  p['header'].get('comment', '__NONE__'),
                'date':     p['header'].get('date', '__NONE__'),
                'discid':   p['header'].get('discid', '__NONE__'),
                'volid':    p['header'].get('volid', '__NONE__') })

def write_tracks_csv(parsed, tracks_csv_file):
    with open(tracks_csv_file, 'w', newline='') as csvfile:
        fieldnames = ['header_ix', 'track_num', 'title', 'performer', 'isrc', 'pregap', 'index_time']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for header_ix, p in enumerate(parsed):
            keys = list(p.keys())
            keys.remove('header')
            for track in sorted(keys):
                if track == "header": 
                    continue
                writer.writerow({
                    'header_ix':    header_ix,
                    'track_num':    track,
                    'title':    p[track].get('title', '__NONE__'),
                    'performer':    p[track].get('performer', '__NONE__'),
                    'isrc':    p[track].get('isrc', '__NONE__'),
                    'pregap':    p[track].get('pregap', '__NONE__'),
                    'index_time':    p[track].get('index', '__NONE__') })


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--recursive', dest='recursive', help="Recursively find .cue files from input path", action='store_true')
    parser.add_argument('-d', '--debug', dest='debug', help="Print debug output", action='store_true')
    parser.add_argument('-H', '--headersfile', dest='headers_csv_file', help="Write headers CSV to specified file", required=False)
    parser.add_argument('-T', '--tracksfile', dest='tracks_csv_file', help="Write tracks CSV to specified file", required=False)
    parser.add_argument('-R', '--rdffile', dest='rdf_file', help='Write to RDF (TTL) file', required=False)
    parser.add_argument('-m', '--mediaroot', dest="media_root_path", help='Media root path, to be overridden in URI generation', required=False)
    parser.add_argument('-q', '--quiet', dest='quiet', help="Suppress printing parse results to terminal", action='store_true')
    parser.add_argument('path', help="Cue file, or folder containing (folders containing) cue files if --recursive specified")
    args = parser.parse_args()

    if not args.recursive and not args.path.endswith(".cue"):
        sys.exit("Specified file is not a cue file. Did you mean to call me with --recursive?")
    elif not args.recursive and not os.path.exists(args.path):
        sys.exit("Could not find specified file")
    cue_files = []
    if args.recursive:
        cue_files = [path for path in pathlib.Path(args.path).rglob('*.cue')]
    else:
        cue_files.append(args.path)
    parsed = [parse_cue_file(cue_file, args.debug) for cue_file in cue_files]
    if args.headers_csv_file:
        write_headers_csv(parsed, args.headers_csv_file)
    if args.tracks_csv_file:
        write_tracks_csv(parsed, args.tracks_csv_file)
    if args.rdf_file:
        if args.recursive:
            media_root_path = args.path
        if args.media_root_path:
            media_root_path = args.media_root_path 
        if not media_root_path:
            sys.exit("Please specify at least one of --recursive or --mediaroot <media_root_path> when writing to RDF")
        write_rdf(parsed, args.rdf_file, media_root_path)
    if not args.quiet:
        pprint(parsed)

