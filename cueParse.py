import argparse, os, sys, pathlib, re, csv, requests, warnings, time, json, shutil
from pprint import pprint
from rdflib import Graph, Literal, RDF, URIRef, BNode
from rdflib.namespace import Namespace, DCTERMS, FOAF, PROV, RDFS, XSD
from typing import Optional
from urllib.parse import quote
from fuzzywuzzy import fuzz
import librosa
import numpy as np
import logging

# --- Logging setup ---
logging.basicConfig(
    filename='cueParse.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

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
# Base SSV namespaces (unbranched defaults)
SSVRelease = Namespace("https://w3id.org/ssv/data/release/")
SSVReleaseEvent = Namespace("https://w3id.org/ssv/data/release_event/")
SSVSignal = Namespace("https://w3id.org/ssv/data/signal/")
SSVRecord = Namespace("https://w3id.org/ssv/data/record/")
SSVTrack = Namespace("https://w3id.org/ssv/data/track/")
SSVPerformance = Namespace("https://w3id.org/ssv/data/performance/")
SSVPerformer = Namespace("https://w3id.org/ssv/data/performer/")
SSVPeaks = Namespace("https://w3id.org/ssv/data/peaks/")
SSVAudio = Namespace("https://w3id.org/ssv/audio/")  # never branched
SSVO = Namespace("https://w3id.org/ssv/vocab#")      # never branched


def get_ssv_namespaces(branch: Optional[str]):
    """
    Return a dict of SSV Namespaces adjusted for the given branch name.
    Branching rule: for namespaces starting with https://w3id.org/ssv/ (data/*),
    prefix becomes https://w3id.org/ssv/{branch}/... except SSVAudio and SSVO which never branch.
    """
    base = "https://w3id.org/ssv/"
    # Normalize branch segment
    if branch:
        branch = branch.strip().strip("/")
        prefix = f"{base}{branch}/"
    else:
        prefix = base

    # Build Namespaces (audio and vocab do not branch)
    return {
        "SSVRelease": Namespace(prefix + "data/release/"),
        "SSVReleaseEvent": Namespace(prefix + "data/release_event/"),
        "SSVSignal": Namespace(prefix + "data/signal/"),
        "SSVRecord": Namespace(prefix + "data/record/"),
        "SSVTrack": Namespace(prefix + "data/track/"),
        "SSVPerformance": Namespace(prefix + "data/performance/"),
        "SSVPerformer": Namespace(prefix + "data/performer/"),
        "SSVPeaks": Namespace(prefix + "data/peaks/"),
        "SSVAudio": Namespace(base + "audio/"),
        "SSVO": Namespace(base + "vocab#"),
    }


def _map_uri_for_branch(u: URIRef, branch: str) -> URIRef:
    base = "https://w3id.org/ssv/"
    if not isinstance(u, URIRef):
        return u
    s = str(u)
    if s.startswith(base):
        # Do not branch audio or vocab
        if s.startswith(base + "audio/") or s.startswith(base + "vocab#"):
            return u
        # Insert branch segment
        return URIRef(base + branch.strip("/") + "/" + s[len(base):])
    return u


def _remap_graph_for_branch(graph: Graph, branch: str) -> Graph:
    """Create a new Graph with all eligible SSV URIs remapped to include the branch."""
    ng = Graph()
    for s, p, o in graph:
        ns = _map_uri_for_branch(s, branch) if isinstance(s, URIRef) else s
        np_ = _map_uri_for_branch(p, branch) if isinstance(p, URIRef) else p
        no = _map_uri_for_branch(o, branch) if isinstance(o, URIRef) else o
        ng.add((ns, np_, no))
    return ng


def bind_pretty_prefixes(graph: Graph, branch: Optional[str]) -> None:
    """Bind readable prefixes so Turtle outputs are cleaner.
    - 'ssv' points to https://w3id.org/ssv/ (main) or https://w3id.org/ssv/{branch}/ (branch)
    - Sub-prefixes for data categories point to .../data/<type>/
    - Audio and vocab are unbranched
    - Common vocabularies (mo, tl, ev, dcterms, rdfs, xsd, foaf, prov, mb*)
    """
    base_root = "https://w3id.org/ssv/"
    base = base_root if not branch else f"{base_root}{branch.strip('/')}/"
    ns = get_ssv_namespaces(branch)

    # Core SSV prefixes
    graph.namespace_manager.bind('ssv', base, override=True)
    graph.namespace_manager.bind('ssvrel', str(ns['SSVRelease']), override=True)
    graph.namespace_manager.bind('ssvrevent', str(ns['SSVReleaseEvent']), override=True)
    graph.namespace_manager.bind('ssvrec', str(ns['SSVRecord']), override=True)
    graph.namespace_manager.bind('ssvtrack', str(ns['SSVTrack']), override=True)
    graph.namespace_manager.bind('ssvsignal', str(ns['SSVSignal']), override=True)
    graph.namespace_manager.bind('ssvperf', str(ns['SSVPerformance']), override=True)
    graph.namespace_manager.bind('ssvperformer', str(ns['SSVPerformer']), override=True)
    graph.namespace_manager.bind('ssvpeaks', str(ns['SSVPeaks']), override=True)
    graph.namespace_manager.bind('ssvaudio', str(ns['SSVAudio']), override=True)  # unbranched
    graph.namespace_manager.bind('ssvo', str(ns['SSVO']), override=True)          # unbranched

    # Music Ontology and friends
    graph.namespace_manager.bind('mo', str(MO), override=False)
    graph.namespace_manager.bind('tl', str(TL), override=False)
    graph.namespace_manager.bind('ev', str(EV), override=False)
    graph.namespace_manager.bind('dcterms', str(DCTERMS), override=False)
    graph.namespace_manager.bind('rdfs', str(RDFS), override=False)
    graph.namespace_manager.bind('xsd', str(XSD), override=False)
    graph.namespace_manager.bind('foaf', str(FOAF), override=False)
    graph.namespace_manager.bind('prov', str(PROV), override=False)

    # MusicBrainz convenience prefixes
    graph.namespace_manager.bind('mbartist', str(ARTIST), override=False)
    graph.namespace_manager.bind('mbwork', str(WORK), override=False)
    graph.namespace_manager.bind('mbrelease', str(RELEASE), override=False)
    graph.namespace_manager.bind('mbisrc', str(ISRC), override=False)
    graph.namespace_manager.bind('mbrec', str(RECORDING), override=False)
    graph.namespace_manager.bind('mbtrack', str(TRACK), override=False)


 

def compute_peaks(audio_path, output_path='peaks.json', segment_size=1024):
    try:
        y, sr = librosa.load(audio_path, sr=None)
    except Exception as e:
        logging.error(f"Error loading audio file: {audio_path} - {e}", exc_info=True)
        return

    abs_y = np.abs(y)
    peaks = [np.max(abs_y[i:i+segment_size]) for i in range(0, len(abs_y), segment_size)]
    
    peaks = np.array(peaks)
    if peaks.max() > peaks.min():  # Avoid division by zero
        peaks = (peaks - peaks.min()) / (peaks.max() - peaks.min())
    else:
        peaks = np.zeros_like(peaks)

    # ensure output_path directory exists
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except Exception as e:
            logging.error(f"Error creating directory {output_dir}: {e}", exc_info=True)
            return
    try:
        with open(output_path, 'w') as f:
            json.dump(peaks.tolist(), f)
        logging.info(f"Peaks saved to {output_path}")
    except Exception as e:
        logging.error(f"Error writing to JSON file: {output_path} - {e}", exc_info=True)
        return

def normalize_path(path):
    # Remove trailing slashes, convert backslashes to slashes, remove redundant escapes
    path = os.path.normpath(path)
    path = path.replace("\\", "/")
    return path.rstrip("/")

def parse_cue_file(file_path, debug):
    try:
        with open(file_path) as file:
            lines = file.readlines()
    except Exception as e:
        logging.error(f"Failed to open cue file {file_path}: {e}", exc_info=True)
        return {}
    parsed = {}
    parsed["file_path"] = file_path
    parsed["header"] = {}
    current_track = None
    current_file = None
    for line in lines:
        line = line.strip()
        if current_track is None and not line.startswith('FILE') and not line.startswith('TRACK'):
            if debug:
                logging.debug("HEADER LINE: " + line)
            mbz_header_artist_match = re.compile ('REM MUSICBRAINZ_ALBUM_ARTIST_ID (.*)').match(line)
            mbz_album_match = re.compile ('REM MUSICBRAINZ_ALBUM_ID (.*)').match(line)
            header_match = re.compile('REM *(.*) (.*)').match(line)
            cat_match = re.compile("CATALOG (.*$)").match(line)
            title_match = re.compile("TITLE (.*$)").match(line)
            perf_match = re.compile("PERFORMER (.*$)").match(line)
            track_match = re.compile(" *TRACK (\\d+) AUDIO").match(line)
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
                logging.debug("skipping line: " + line)
        else:
            if debug:
                logging.debug("BODY LINE: " + line)
            mbz_track_match = re.compile(" *REM MUSICBRAINZ_TRACK_ID (.*$)").match(line)
            mbz_artist_match = re.compile(" *REM MUSICBRAINZ_ARTIST_ID (.*$)").match(line)
            title_match = re.compile(" *TITLE (.*$)").match(line)
            perf_match = re.compile(" *PERFORMER (.*$)").match(line)
            isrc_match = re.compile(" *ISRC (.*$)").match(line)
            pregap_match = re.compile(" *PREGAP (.*$)").match(line)
            index_match = re.compile(" *INDEX 01 (.*$)").match(line)
            file_match = re.compile('FILE "(.*)" WAVE$').match(line)
            track_match = re.compile(" *TRACK (\\d+) AUDIO").match(line)
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
                if current_file:
                    parsed[current_track]["file"] = current_file
                else:
                    logging.warning("No file found for track " + str(current_track))
            elif file_match:
                # FILE line comes before TRACK line, so hold on to the file name until we see a TRACK line
                logging.info("Current track " + str(current_track) + ", match: " + str(file_match[1]))
                current_file = file_match[1]
            elif debug: 
                logging.debug("skipping line: " + line)
    return parsed

 


def build_rdf_content(parsed, media_root_paths, peaks_root_dir: Optional[str]):
    """
    Build the canonical (unbranched) RDF content once.
    Returns: (full_graph, private_graph, locals_dict)
    - locals_dict: {subdir: [(graph, ssvUriComponent), ...]}
    Peaks files are written only if peaks_root_dir is provided.
    """
    g = Graph()
    private = Graph()
    locals_dict: dict[str, list[tuple[Graph, str]]] = {s: [] for s in [
        "release", "release_event", "record", "track", "signal", "performance", "performer"
    ]}

    normalized_roots = [normalize_path(root) for root in media_root_paths]
    logging.info("Normalized media roots: %s", normalized_roots)

    # Use unbranched namespaces for building
    ns = get_ssv_namespaces(None)
    SSVRelease = ns["SSVRelease"]
    SSVReleaseEvent = ns["SSVReleaseEvent"]
    SSVSignal = ns["SSVSignal"]
    SSVRecord = ns["SSVRecord"]
    SSVTrack = ns["SSVTrack"]
    SSVPerformance = ns["SSVPerformance"]
    SSVPerformer = ns["SSVPerformer"]
    SSVPeaks = ns["SSVPeaks"]
    SSVAudio = ns["SSVAudio"]
    SSVO = ns["SSVO"]

    for p in parsed:
        file_parent = normalize_path(pathlib.Path(p['file_path']).parent.as_posix())
        best_root = None
        best_len = -1
        for root in normalized_roots:
            root_slash = root if root.endswith("/") else root + "/"
            file_parent_slash = file_parent if file_parent.endswith("/") else file_parent + "/"
            if file_parent_slash.startswith(root_slash) and len(root_slash) > best_len:
                logging.info(f"Matching media root {root} for file {p['file_path']}")
                best_root = root
                best_len = len(root_slash)
            else:
                logging.info(f"Not matching media root {root} for file {p['file_path']}")
        if not best_root:
            logging.warning(f"No matching media root for file {p['file_path']}. Using first provided root.")
            best_root = normalized_roots[0]
        root_slash = best_root if best_root.endswith("/") else best_root + "/"
        rel_path = file_parent[len(root_slash.rstrip("/")):]
        rel_path = rel_path.lstrip("/\\")
        ssvUriComponent = quote(rel_path).replace('/', '__').replace(' ','_').replace('%', '_-')

        release = URIRef(SSVRelease + str(ssvUriComponent))
        release_event = URIRef(SSVReleaseEvent + str(ssvUriComponent))
        record = URIRef(SSVRecord + str(ssvUriComponent))
        mbz_album_json = None
        if 'mbz_album_id' in p['header']:
            time.sleep(0.3)
            try:
                r = requests.get("https://musicbrainz.org/album/" + p['header']['mbz_album_id'], headers={"Accept": "application/ld+json"})
                r.raise_for_status()
                logging.info("MusicBrainz response for album id %s: %s", p['header']['mbz_album_id'], r.text)
                mbz_album_json = r.json()
            except requests.exceptions.HTTPError as err:
                logging.warning("Could not GET Musicbrainz album %s: %s", p['header']['mbz_album_id'], err)
            except Exception as e:
                logging.error(f"Error fetching MusicBrainz album {p['header']['mbz_album_id']}: {e}", exc_info=True)

        # RELEASE
        releaseGraph = Graph()
        releaseGraph.add((release, RDF.type, MO.Release))
        releaseGraph.add((release, DCTERMS.title, Literal(p['header'].get('title', '__NONE__'))))
        releaseGraph.add((release, RDFS.label, Literal("Release: " + p['header'].get('title', '__NONE__'))))
        releaseGraph.add((release, MO.catalogue_number, Literal(p['header'].get('catalogue_number', '__NONE__'))))
        releaseGraph.add((release, MO.record, record))
        g += releaseGraph
        locals_dict["release"].append((releaseGraph, ssvUriComponent))

        # RELEASE EVENT
        releaseEventGraph = Graph()
        releaseEventGraph.add((release_event, RDF.type, MO.ReleaseEvent))
        releaseEventGraph.add((release_event, RDF.type, EV.Event))
        releaseEventGraph.add((release_event, MO.release, release))
        release_event_time = BNode()
        releaseEventGraph.add((release_event, EV.time, release_event_time))
        releaseEventGraph.add((release_event_time, RDF.type, TL.Instant))
        releaseEventGraph.add((release_event_time, TL.atYear, Literal(p['header'].get('date', '__NONE__'), datatype=XSD.gYear)))
        g += releaseEventGraph
        locals_dict["release_event"].append((releaseEventGraph, ssvUriComponent))

        # RECORD, TRACK, SIGNAL
        recordGraph = Graph()
        trackGraph = Graph()
        signalGraph = Graph()
        recordGraph.add((record, RDF.type, MO.Record))
        releaseGraph.add((release, RDFS.label, Literal("Record: " + p['header'].get('title', '__NONE__'))))
        recordGraph.add((record, MO.track_count, Literal(len(p)-1)))
        if 'musicbrainz_album_id' in p['header']:
            recordGraph.add((record, MO.musicbrainz, RELEASE.p['header']['musicbrainz_album_id']))

        for track_num in p:
            if track_num == 'header' or track_num == 'file_path':
                continue
            tix = str(ssvUriComponent) + '#' + str(track_num)
            track = URIRef(SSVTrack + tix)
            signal = URIRef(SSVSignal + tix)
            performance = URIRef(SSVPerformance + tix)
            performer = URIRef(SSVPerformer + tix)

            recordGraph.add((record, MO.track, track))
            releaseGraph.add((release, MO.publication_of, signal))

            # SIGNAL
            audioUri = ""
            if 'file' in p[track_num] and p[track_num]['file'] != "__SKIP__":
                audio_path = os.path.join(pathlib.Path(p['file_path']).parent, pathlib.Path(p[track_num]['file'].replace('\\','/')).name)
                audio_path = audio_path.strip()
                if peaks_root_dir:
                    output_path = os.path.join(peaks_root_dir, 'peaks', ssvUriComponent, str(track_num) + '.peaks.json')
                else:
                    output_path = None
                logging.info("Audio path: |%s|", audio_path)
                if os.path.exists(audio_path) and output_path:
                    compute_peaks(audio_path, output_path)
                    signalGraph.add((signal, SSVO.peaks, URIRef(SSVPeaks + str(ssvUriComponent) + '/' + str(track_num) + '.peaks.json')))
                    audioUri = URIRef(str(SSVAudio) + str(ssvUriComponent) + "/" + quote(pathlib.Path(audio_path).name))
                elif not os.path.exists(audio_path):
                    logging.warning("Audio file not found: %s", audio_path)
            else:
                logging.warning("No file in track_num: %s", str(track_num))
            signalGraph.add((signal, RDF.type, MO.Signal))
            signalGraph.add((signal, MO.published_as, track))
            if 'isrc' in p[track_num]:
                isrc = p[track_num]['isrc']
                signalGraph.add((signal, MO.isrc, URIRef(ISRC + isrc)))

            # TRACK
            trackGraph.add((track, RDF.type, MO.Track))
            private.add((track, RDF.type, MO.Track))
            if 'mbz_track' in p[track_num]:
                mbz_track_id = str(p[track_num]['mbz_track'])
                trackGraph.add((track, MO.musicbrainz, URIRef(TRACK + mbz_track_id)))
                private.add((track, MO.musicbrainz, URIRef(TRACK + mbz_track_id)))
            trackGraph.add((track, MO.track_number, Literal(int(track_num))))
            private.add((track, MO.track_number, Literal(int(track_num))))
            trackGraph.add((track, RDFS.label, Literal("Track: " + p[track_num]["title"])))
            private.add((track, RDFS.label, Literal("Track: " + p[track_num]["title"])))
            private.add((track, SSVO.localPath, Literal(p[track_num].get("file", "__NONE__"))))
            if audioUri:
                trackGraph.add((track, MO.available_as,audioUri))

            # WORK via MBZ
            work = None
            if mbz_album_json:
                mbz_tracks_json = mbz_album_json.get('track', [])
                try:
                    mbz_track_json = [t for t in mbz_tracks_json if t['trackNumber'][t['trackNumber'].index(".")+1:] == str(track_num)]
                except Exception as e:
                    logging.error("Unexpected trackNumber format: %s", e, exc_info=True)
                    continue
                if len(mbz_track_json) > 1:
                    similarities = [fuzz.ratio(t['name'], p[track_num]['title']) for t in mbz_track_json]
                    close_match_indices = [ix for ix, val in enumerate(similarities) if val > 90]
                    mbz_track_json = [mbz_track_json[i] for i in close_match_indices]
                if len(mbz_track_json) > 1:
                    logging.warning("Multiple matches on track disambiguation, please sort manually: %s ##### %s", mbz_track_json, p[track_num])
                if len(mbz_track_json) == 0:
                    logging.warning("Can't find unique matching cue track name %s", p[track_num]["title"]) 
                else:
                    if 'recordingOf' in mbz_track_json[0]:
                        rec = mbz_track_json[0]['recordingOf']
                        if isinstance(rec, list):
                            logging.warning("Track associated with multiple works: %s", mbz_track_json[0].get("@id", ""))
                        else:
                            rec = [rec]
                        for r in rec:
                            work = URIRef(r['@id'])
                            g.add((work, RDF.type, MO.MusicalWork))
                            g.add((work, DCTERMS.title, Literal(r['name'])))
                            g.add((work, RDFS.label, Literal("Work: " + r['name'])))
                    else:
                        logging.warning("No work associated with MBz track: %s", mbz_track_json[0].get("@id", ""))

            # PERFORMANCE
            performanceGraph = Graph()
            performanceGraph.add((performance, RDF.type, MO.Performance))
            performanceGraph.add((performance, MO.recorded_as, signal))
            if work:
                performanceGraph.add((performance, MO.performance_of, work))
            performanceGraph.add((performance, RDFS.label, Literal("Performance: " + p[track_num]["title"])))

            # PERFORMER
            performerGraph = Graph()
            performerGraph.add((performer, RDF.type, MO.MusicArtist))
            performerGraph.add((performer, MO.performed, performance))
            performerGraph.add((performer, FOAF.name, Literal(p[track_num]["performer"])))
            performerGraph.add((performer, RDFS.label, Literal("Performer: " + p[track_num]["performer"])))
            if 'mbz_artist' in p[track_num]:
                mbz_artist_ids = p[track_num]['mbz_artist'].split("; ")
                for mbz_artist_id in mbz_artist_ids:
                    performerGraph.add((performer, MO.musicbrainz, URIRef(ARTIST + mbz_artist_id.replace('"', ''))))

            # Collect locals
            locals_dict["record"].append((recordGraph, ssvUriComponent))
            locals_dict["track"].append((trackGraph, ssvUriComponent))
            locals_dict["signal"].append((signalGraph, ssvUriComponent))
            locals_dict["performance"].append((performanceGraph, ssvUriComponent))
            locals_dict["performer"].append((performerGraph, ssvUriComponent))

            # Merge into global graph
            g += releaseGraph
            g += releaseEventGraph
            g += recordGraph
            g += trackGraph
            g += signalGraph
            g += performanceGraph
            g += performerGraph

    return g, private, locals_dict

def serializeRdf(g, path):
    try:
        # serialize the graph to a file in each of the formats
        g.serialize(destination=path+'.ttl', format="text/turtle")
        g.serialize(destination=path+'.rdf', format="application/rdf+xml")
        g.serialize(destination=path+'.jsonld', format="json-ld")
        g.serialize(destination=path+'.n3', format="n3")
        g.serialize(destination=path+'.nt', format="nt")
        logging.info(f"Serialized RDF graphs to {path}.*")
    except Exception as e:
        logging.error(f"Error serializing RDF to {path}: {e}", exc_info=True)

def write_headers_csv(parsed, headers_csv_file):
    try:
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
        logging.info(f"Wrote headers CSV to {headers_csv_file}")
    except Exception as e:
        logging.error(f"Error writing headers CSV to {headers_csv_file}: {e}", exc_info=True)

def write_tracks_csv(parsed, tracks_csv_file):
    try:
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
        logging.info(f"Wrote tracks CSV to {tracks_csv_file}")
    except Exception as e:
        logging.error(f"Error writing tracks CSV to {tracks_csv_file}: {e}", exc_info=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--recursive', dest='recursive', help="Recursively find .cue files from input path", action='store_true')
    parser.add_argument('-d', '--debug', dest='debug', help="Print debug output", action='store_true')
    parser.add_argument('-H', '--headersfile', dest='headers_csv_file', help="Write headers CSV to specified file", required=False)
    parser.add_argument('-T', '--tracksfile', dest='tracks_csv_file', help="Write tracks CSV to specified file", required=False)
    parser.add_argument('-R', '--rdffile', dest='rdf_file', help='Write to RDF (TTL) file', required=False)
    parser.add_argument('-D', '--rdfdirectory', dest='rdf_directory', help='Write RDF (TTL) files to specified directory', required=False)
    parser.add_argument('-A', '--audioFilenameRdf', dest='audio_filename_rdf', help='Write private audio file to track URI information to RDF (TTL) file', required=False)
    parser.add_argument('-m', '--mediaroot', dest="media_root_paths", help='Media root path, to be overridden in URI generation', required=False, action='append')
    parser.add_argument('-b', '--branch', dest='branches', help='Branch name to include in SSV data namespaces; repeatable', required=False, action='append')
    parser.add_argument('-q', '--quiet', dest='quiet', help="Suppress printing parse results to terminal", action='store_true')
    parser.add_argument('path', help="Cue file, or folder containing (folders containing) cue files if --recursive specified")
    args = parser.parse_args()

    if not args.recursive and not args.path.endswith(".cue"):
        logging.error("Specified file is not a cue file. Did you mean to call me with --recursive?")
        sys.exit(1)
    elif not args.recursive and not os.path.exists(args.path):
        logging.error("Could not find specified file: %s", args.path)
        sys.exit(1)
    cue_files = []
    if args.recursive:
        cue_files = [str(path) for path in pathlib.Path(args.path).rglob('*picard*.cue')]
    else:
        cue_files.append(args.path)
    parsed = []
    for cue_file in cue_files:
        result = parse_cue_file(cue_file, args.debug)
        if result:
            parsed.append(result)
        else:
            logging.warning(f"Skipping file due to parse error: {cue_file}")
    if args.headers_csv_file:
        write_headers_csv(parsed, args.headers_csv_file)
    if args.tracks_csv_file:
        write_tracks_csv(parsed, args.tracks_csv_file)
    if args.rdf_file:
        # Determine media roots
        rdf_dir_path = False
        if args.rdf_directory:
            rdf_dir_path = args.rdf_directory
        audio_filename_rdf = False
        media_root_paths = []
        if args.recursive:
            media_root_paths.append(args.path)
        if args.media_root_paths:
            media_root_paths.extend(args.media_root_paths)
        if not media_root_paths:
            logging.error("Please specify at least one of --recursive or --mediaroot <media_root_path> when writing to RDF")
            sys.exit(1)
        if args.audio_filename_rdf:
            audio_filename_rdf = args.audio_filename_rdf

        branches = args.branches or []

        # Build canonical graphs once (unbranched). Peaks written into appropriate root if per-entity dir exists.
        # Choose a peaks root: if we have a per-entity dir, use it; else use dirname of rdf_file.
        if branches:
            base_out_dir = rdf_dir_path if rdf_dir_path else (os.path.dirname(args.rdf_file) or ".")
            peaks_root = os.path.join(base_out_dir, "main")  # compute peaks under main
        else:
            peaks_root = rdf_dir_path if rdf_dir_path else (os.path.dirname(args.rdf_file) or ".")

        full_graph, private_graph, locals_dict = build_rdf_content(parsed, media_root_paths, peaks_root_dir=peaks_root)

        def write_aggregate(graph: Graph, out_path: str, branch_ctx: Optional[str]):
            parent = os.path.dirname(out_path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)
            bind_pretty_prefixes(graph, branch_ctx)
            serializeRdf(graph, out_path)

        if branches:
            # write main (unbranched)
            base_rdf_name = os.path.basename(args.rdf_file)
            out_dir_main = os.path.join(base_out_dir, "main")
            agg_main = os.path.join(out_dir_main, base_rdf_name)
            write_aggregate(full_graph, agg_main, None)
            if audio_filename_rdf:
                write_aggregate(private_graph, os.path.join(out_dir_main, os.path.basename(audio_filename_rdf)), None)
            # per-entity for main
            if args.rdf_directory:
                for subdir, items in locals_dict.items():
                    subdir_path = os.path.join(out_dir_main, subdir)
                    os.makedirs(subdir_path, exist_ok=True)
                    for local_graph, key in items:
                        bind_pretty_prefixes(local_graph, None)
                        serializeRdf(local_graph, os.path.join(subdir_path, key))

            # Now branch serializations by remapping URIs only
            for branch in branches:
                out_dir = os.path.join(base_out_dir, branch)
                agg_path = os.path.join(out_dir, base_rdf_name)
                # Remap aggregate
                branched_graph = _remap_graph_for_branch(full_graph, branch)
                write_aggregate(branched_graph, agg_path, branch)
                # Private graph should remain unbranched (contains local paths and MBZ links)
                if audio_filename_rdf:
                    write_aggregate(private_graph, os.path.join(out_dir, os.path.basename(audio_filename_rdf)), None)
                # Per-entity
                if args.rdf_directory:
                    for subdir, items in locals_dict.items():
                        subdir_path = os.path.join(out_dir, subdir)
                        os.makedirs(subdir_path, exist_ok=True)
                        for local_graph, key in items:
                            remapped = _remap_graph_for_branch(local_graph, branch)
                            bind_pretty_prefixes(remapped, branch)
                            serializeRdf(remapped, os.path.join(subdir_path, key))
                # Copy peaks files from main into each branch folder
                # We only need to replicate files on disk; URIs in RDF are remapped via graph.
                peaks_src = os.path.join(out_dir_main, 'peaks')
                peaks_dst = os.path.join(out_dir, 'peaks')
                if os.path.exists(peaks_src):
                    try:
                        if os.path.exists(peaks_dst):
                            shutil.rmtree(peaks_dst)
                        shutil.copytree(peaks_src, peaks_dst)
                    except Exception as e:
                        logging.error(f"Error copying peaks from {peaks_src} to {peaks_dst}: {e}", exc_info=True)
        else:
            # No branches -> write as before, single set
            out_path = args.rdf_file
            write_aggregate(full_graph, out_path, None)
            if audio_filename_rdf:
                write_aggregate(private_graph, audio_filename_rdf, None)
            if args.rdf_directory:
                for subdir, items in locals_dict.items():
                    subdir_path = os.path.join(rdf_dir_path, subdir) if rdf_dir_path else os.path.join(os.path.dirname(out_path) or '.', subdir)
                    os.makedirs(subdir_path, exist_ok=True)
                    for local_graph, key in items:
                        bind_pretty_prefixes(local_graph, None)
                        serializeRdf(local_graph, os.path.join(subdir_path, key))
    if not args.quiet: 
        pprint(parsed)
