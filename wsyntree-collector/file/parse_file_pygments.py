import pygments
import pygments.lexers
import pathlib

from pygments import token
from pygments.token import Token

'''
Utilities for plotting code features as a heatmap based on a
code repository dataframe.

Utilities for encoding code features as a lossy heatmap for transmission
to upstream
'''

# get the idx's for newlines
# very c style
def get_newline_indices(s):
    last_newline_idx = -1
    newline_idx_vec = []
    while True:
        last_newline_idx = s.find('\n', last_newline_idx+1)
        if last_newline_idx == -1:
            break
        newline_idx_vec.append(last_newline_idx)
    return newline_idx_vec

def get_tokens_for_file(filepath):
    """Get the list of tokens for the file.

    If there is no Pygments lexer for the file extension then returns `None`

    We cannot / shall never use a cache here so that we can be 'pure' for
    the module _foreach_gitfile function to work without a data race.
    Performance sacrifice is not a primary concern.
    Also the memory footprint of a cache is impractical for large repos.
    Any cache that isn't cross-process is even more useless since most modules
    only get the result for one file once.
    """
    path = pathlib.Path(filepath)
    try:
        lxr = pygments.lexers.get_lexer_for_filename(path.name)
    except pygments.util.ClassNotFound as e:
        return None
    with path.open('r') as f:
        try:
            return lxr.get_tokens_unprocessed(f.read())
        except UnicodeDecodeError as e:
            return None

'''
Written to use pygments, however, it should apply to anything that has the format idx, type, val or len.
Would be a bit more trivial if pygments indexed their data by line.
The resulting array is going to be large, ideally results will be aggregated across all repos (just add the np arrays)
TODO: replace any CRLF with LF
'''
def file_tokens_to_frequency_count(input_path, static_frequency_array=None, src_resolution_x=250,src_resolution_y=2000,
        target_token_types=pygments.token.STANDARD_TYPES.keys(), 
        exclude_token_types = [Token.Text.Whitespace, Token.Literal.String.Doc]):

    raise NotImplementedError

    tokens = get_tokens_for_file(input_path)
    if tokens is None:
        return None

    token_types = list(set(target_token_types) - set(exclude_token_types))
    newline_indices = get_newline_indices(input_path)
    pygment_token_ids = {k:i for i,k in enumerate(token_types)}

    y_start=0
    # TODO: NOT IMPLEMENTED
    df_dict = {}

    for (idx, tokentype, value) in get_tokens_for_file(input_path):

        # allow for inclusion/exclusion of token types
        if tokentype not in token_types:
            continue
        # pass by newlines until we reach one that
        # TODO: consider if idx == newline indices 
        # lmao write a C program to get around the GIL
        while True:
            if (y_start >= len(newline_indices)) or (y_start >= src_resolution_y):
                break

            if idx < newline_indices[y_start]:
                break
            y_start+=1

        y_end=y_start
        x_start = idx
        x_end = x_start+len(value)

        if y_start>0:
            # store the token data in the first line 
            y_start -= 1
            x_start -= newline_indices[y_start]+1
            x_end -= newline_indices[y_start]+1
        
        # TODO: consider idx+len == ending newline index
        # should be true when a newline is present in the value
        while True:
            # check that the ending x fits in the dimension - as of now, this information is discarded
            if x_end > src_resolution_x:
                x_end = src_resolution_x

            if (y_end >= len(newline_indices)) or (y_end >= src_resolution_y):
                break

            if idx + len(value) < newline_indices[y_end]:
                break

            x_end = newline_indices[y_end]
            if y_end > 0:
                x_end -= newline_indices[y_end-1]

            # static_frequency_array[y_end][x_start:x_end, pygment_token_ids[tokentype]]+=1
            x_start = 0
            y_end+=1
            
        # static_frequency_array[y_end][x_start:x_end, pygment_token_ids[tokentype]]+=1

    return None
