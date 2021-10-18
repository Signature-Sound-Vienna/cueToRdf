import argparse, os, sys, pathlib, re

def parse_cue_file(file_path, debug):
    with open(file_path) as file:
        lines = file.readlines()
        parsed = {}
        parsed["header"] = {}
        current_track = None
        for line in lines:
            line = line.strip()
            if(current_track is None):
                print(line)
                header_match = re.compile("REM (\S+) (.*$)").match(line)
                cat_match = re.compile("CATALOG (.*$)").match(line)
                title_match = re.compile("TITLE (.*$)").match(line)
                perf_match = re.compile("PERFORMER (.*$)").match(line)
                track_match = re.compile("\s+ TRACK (\d+) AUDIO").match(line)
                if header_match:
                    parsed["header"][header_match[1].lower()] = header_match[2]
                elif cat_match:
                    parsed["header"]["catalog"] = cat_match[1]
                elif title_match:
                    parsed["header"]["title"] = title_match[1]
                elif perf_match:
                    parsed["header"]["performer"] = perf_match[1]
                elif track_match:
                    current_track = int(track_match[1])
                elif debug: 
                    print("skipping line: ", line)
            else:
                title_match = re.compile("\s* TITLE (.*$)").match(line)
                perf_match = re.compile("\s* PERFORMER (.*$)").match(line)
                isrc_match = re.compile("\s* ISRC (.*$)").match(line)
                pregap_match = re.compile("\s* ISRC (.*$)").match(line)
                index_match = re.compile("\s* INDEX O1 (.*$)").match(line)
                track_match = re.compile("\s* TRACK (\d+) AUDIO").match(line)
                if title_match:
                    parsed[current_track]["title"] = title_match[1]
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
                elif debug: 
                    print("skipping line: ", line)
    return parsed

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--recursive', dest='recursive', help="Recursively find .cue files from input path", action='store_true')
    parser.add_argument('-d', '--debug', dest='debug', help="Print debug output", action='store_true')
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
    if args.debug:
        print("*** Cue files: ", cue_files)
    parsed = [parse_cue_file(cue_file, args.debug) for cue_file in cue_files]
    print("Parsed: ", parsed)

        


