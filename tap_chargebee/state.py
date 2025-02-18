
import json
import singer

from datetime import datetime, timedelta

LOGGER = singer.get_logger()


def get_last_record_value_for_table(state, table, field):
    last_value = state.get('bookmarks', {}) \
                      .get(table, {}) \
                      .get(field)

    if last_value is None:
        return None

    if isinstance(last_value, str):
        try:
            last_value = datetime.strptime(last_value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return last_value  # Return as is if parsing fails

    if isinstance(last_value, datetime):
        return (last_value + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    return last_value  # Return as is if it's not a datetime object

def incorporate(state, table, key, value, force=False):
    if value is None:
        return state

    if isinstance(value, datetime):
        value = value.strftime('%Y-%m-%dT%H:%M:%SZ')

    if state is None:
        new_state = {}
    else:
        new_state = state.copy()

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if table not in new_state['bookmarks']:
        new_state['bookmarks'][table] = {}

    if(new_state['bookmarks'].get(table, {}).get(key) is None or
       new_state['bookmarks'].get(table, {}).get(key) < value or
       force):
        new_state['bookmarks'][table][key] = value

    return new_state


def save_state(state):
    if not state:
        return

    LOGGER.info('Updating state.')

    singer.write_state(state)


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as handle:
            return json.load(handle)
    except:
        LOGGER.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError
