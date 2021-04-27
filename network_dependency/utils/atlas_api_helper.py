import logging


def get_dst_addr(result: dict) -> str:
    if 'fw' not in result:
        logging.error('Malformed result (fw key is missing): {}'
                      .format(result))
        return str()
    fw = result['fw']
    if fw < 4400:
        logging.error('Result parsing for fw < 4400 is not implemented: {}'
                      .format(result))
        return str()
    if 4400 <= fw < 4460:
        if 'addr' not in result:
            logging.debug('No destination address in result (maybe DNS '
                          'resolution failed): {}'.format(result))
            return str()
        return result['addr']
    if 4460 <= fw:
        if 'dst_addr' not in result:
            logging.debug('No destination address in result (maybe DNS '
                          'resolution failed): {}'.format(result))
            return str()
        return result['dst_addr']
