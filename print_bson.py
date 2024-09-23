import bson
import argparse
import sys
import struct


def unpack_dictionaries(doc):
    # Flatten dictionaries
    while True:
        dictcols = [k for k, v in doc.items() if isinstance(v, dict)]
        if len(dictcols) == 0:
            break
        doc.update({f'{k}.{kk}': vv for k, v in doc.items() if isinstance(v, dict) for kk, vv in v.items()})
        for i in dictcols:
            del doc[i] 
    return doc

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                        prog='print_bson',
                        description="Prints a bunch of BSON lines to stdout")
    parser.add_argument('input') 
    parser.add_argument('-s', '--skip', type=int, default=1,
                        help='''Prints every nth line.
                        Example: --skip 100 to print every 100th line''')
    parser.add_argument('-f', '--flatten', action='store_true',
                        help='''Flatten dictionaries''')
    parser.add_argument('-w', '--wait', action='store_true',
                        help='''Wait for user input before printing the next line''')
    args = parser.parse_args()
    l = 0
    with open(args.input, 'rb') as f:
        parquet_writer = None
        chunk = []
        
        while True:
            try:
                # Read the first 4 bytes to get the document size (BSON documents are prefixed with their size)
                size_data = f.read(4)
                
                if len(size_data) == 0:
                    # EOF reached
                    break

                # Get the size of the BSON document
                doc_size = struct.unpack("<i", size_data)[0]

                
                # Read the entire BSON document
                doc_data = size_data + f.read(doc_size - 4)
                # Decode the BSON document
                doc = bson.BSON.decode(doc_data)
                # flatten dictionaries
                if args.flatten:
                    doc = unpack_dictionaries(doc)
                l += 1
                if l % args.skip == 0:
                    print(doc)
                    if args.wait:
                        input("Press Enter to continue...")
            except Exception as e:
                print(f"Error processing document: {e}")
                break
