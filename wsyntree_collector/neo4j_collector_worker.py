
from pathlib import Path

import neo4j

from wsyntree import log
from wsyntree.tree_models import (
    SCM_Host, WSTRepository, File, WSTNode, WSTText, WSTUniqueText, WSTHugeText
)
from wsyntree.wrap_tree_sitter import get_TSABL_for_file


def _process_file(path: Path, tree_repo: WSTRepository):
    file = File(
        path=str(path)
    )
    # file.save()

    lang = get_TSABL_for_file(file.path)
    if lang is None:
        # log.debug(f"no language available for {file}")
        file.error = "NO_LANGUAGE"
        file.save()
        file.repo.connect(tree_repo)
        return
    else:
        file.language = lang.lang
        file.save()
        file.repo.connect(tree_repo)

    tree = lang.parse_file(file.path)

    # log.debug(f"growing nodes for {file}")
    cursor = tree.walk()
    # iteration loop
    cur_tree_parent = None
    while cursor.node is not None:
        cur_node = cursor.node
        nn = WSTNode(
            named=cur_node.is_named,
            type=cur_node.type,# if cur_node.is_named else None,
            # children=[],
        )
        (nn.x1,nn.y1) = cur_node.start_point
        (nn.x2,nn.y2) = cur_node.end_point
        nn.save()
        nn.file.connect(file)
        if cur_tree_parent:
            nn.parent.connect(cur_tree_parent)
        # text storage
        text = None
        try:
            decoded_content = cur_node.text.tobytes().decode()
            if len(decoded_content) <= 4e3:
                text = WSTUniqueText.get_or_create({
                    'text': decoded_content,
                    'length': len(decoded_content)
                })[0]
            else:
                text = WSTHugeText(
                    text=decoded_content,
                    length=len(decoded_content)
                )
                text.save()
            nn.text.connect(text)
        except neo4j.exceptions.DatabaseError as e:
            # Neo4j imposes an internal hard limit of 4039 bytes to string properties
            # https://neo4j.com/docs/operations-manual/current/performance/index-configuration/#index-configuration-btree-limitations-key-sizes
            log.error(f"{file}:{nn} text content not stored: {e}")
            file.error = "WST_TEXT_STORE_FAILED"
            file.save()
            raise e
        except UnicodeDecodeError as e:
            log.warn(f"{file}:{nn} failed to decode content")
            file.error = "UNICODE_DECODE_ERROR"
            file.save()
            return

        # now determine where to move to next:
        next_child = cursor.goto_first_child()
        if next_child == True:
            cur_tree_parent = nn
            continue # cur_node to next_child
        next_sibling = cursor.goto_next_sibling()
        if next_sibling == True:
            continue # cur_node to next_sibling
        # go up parents
        while cursor.goto_next_sibling() == False:
            goto_parent = cursor.goto_parent()
            if goto_parent:
                # reversing up the tree
                if cur_tree_parent.parent.get_or_none():
                    cur_tree_parent = cur_tree_parent.parent.get()
                else:
                    cur_tree_parent = None
            else:
                # we are done iterating
                return goto_parent
