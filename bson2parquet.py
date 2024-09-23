import bson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import struct
import pandas as pd

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


def bson_infer_col(bson_file_path: str, limit: int):
    # Open the BSON file
    l = 0
    print("Inferring columns")
    printed = []
    colnames = set() 
    with open(bson_file_path, 'rb') as f:
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
                l += 1
                if l % 100000 == 0:
                    print(l)

                # Decode the BSON document
                doc = bson.BSON.decode(doc_data)
                doc = unpack_dictionaries(doc)
                for i in doc:
                    colnames.add(i)

                if limit is not None and l >= limit:
                    break
            except Exception as e:
                print(f"Error processing document: {e}")
                import pdb
                pdb.set_trace()
                break
    print("Inferring columns done")
    return colnames



def bson_to_parquet_chunked(bson_file_path: str, parquet_file_path: str, colnames: set, intcols: set, limit: int, chunk_size: int = 1000):
    """
    Converts a BSON file to a Parquet file by processing it in chunks.

    Parameters:
    - bson_file_path: str - The path to the BSON file to be converted.
    - parquet_file_path: str - The path to the output Parquet file.
    - chunk_size: int - The number of documents to process in each chunk (default: 1000).
    """
    # Open the BSON file
    l = 0
    printed = []
    with open(bson_file_path, 'rb') as f:
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
                l += 1
                if l % 100000 == 0:
                    print("Reading")
                    print(l)

                # Decode the BSON document
                doc = bson.BSON.decode(doc_data)
                # flatten dictionaries
                doc = unpack_dictionaries(doc)
                # Remove columns not in colnames
                for i in list(doc.keys()):
                    if i not in colnames:
                        del doc[i]
                for i in doc:
                    doc[i] = str(doc[i])
                # Add columns in colnames that are not in doc
                # force everything to string
                # otherwise the arrow type inference gets mad
                # and makes the column null if the the entire batch does 
                # not contain this column
                for i in colnames:
                    if i not in doc:
                        doc[i] = ''
                # special case 'size'
                for i in intcols:
                    try:
                        doc[i] = int(doc[i])
                    except:
                        doc[i] = 0
                chunk.append(doc)

                if limit is not None and l >= limit:
                    break
                
                # If chunk is full, process it
                if len(chunk) >= chunk_size:
                    # Convert chunk to DataFrame
                    df = pd.DataFrame(chunk, index=None)

                    cols = sorted(df.columns.tolist())
                    df = df[cols]
                    print(df)
                    
                    # Convert DataFrame to Arrow Table
                    table = pa.Table.from_pandas(df)

                    # Write the chunk to Parquet
                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(parquet_file_path, table.schema)
                    parquet_writer.write_table(table)
                    print("Writing")
                    print(l)

                    # Clear the chunk
                    chunk = []
            
            except Exception as e:
                print(f"Error processing document: {e}")
                import pdb
                pdb.set_trace()
                break

        # Final processing for the last chunk
        if chunk:
            df = pd.DataFrame(chunk, index=None)
            cols = sorted(df.columns.tolist())
            df = df[cols]
            print(df)
            table = pa.Table.from_pandas(df)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(parquet_file_path, table.schema)
            parquet_writer.write_table(table)

        # Close the Parquet writer
        if parquet_writer:
            parquet_writer.close()

    print(f"Successfully written BSON data to {parquet_file_path} in chunks of {chunk_size} documents.")
# Example usage

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser(
                        prog='bson2parquet',
                        description='''Converts BSON dump (from mongo) to parquet.
                                Automatically recursively flattens dictionaries.
                                Lists are not supported. ''')
    parser.add_argument('input')           # positional argument
    parser.add_argument('output')           # positional argument
    parser.add_argument('-x', '--exclude', type=str, action="append", 
                        help='''if column name includes this substring it is excluded. 
                        This option can be repeated. 
                        Example: -x secrets''')
    parser.add_argument('-i', '--integer', type=str, action="append", 
                        help='''if column name is this string it is forced to be integer. 
                        This option can be repeated. 
                        Example: -i size''')
    parser.add_argument('-l', '--limit', type=int,  
                        help='''Maximum number of rows to process''')
    args = parser.parse_args()
    print(args)
    exclude = args.exclude
    if exclude is None:
        exclude = []
    integers = args.integer
    if integers is None:
        integers = []
    print("Performing first pass")
    colnames = bson_infer_col(args.input, args.limit)
    for i in list(colnames):
        for j in exclude:
            if j in i:
                colnames.remove(i)
    print("-------------------------")
    print("Columns")
    print(colnames)
    print("-------------------------")
    print("Performing second and final pass")
    bson_to_parquet_chunked(args.input, args.output, colnames, set(integers), args.limit,chunk_size=100000)
