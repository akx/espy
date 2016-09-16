from collections import defaultdict
from itertools import cycle
from urllib.parse import urljoin

import click
import requests


class Es(requests.Session):
    base_url = 'http://127.0.0.1:9200/'

    def request(self, method, url, **kwargs):
        if '://' not in url:
            url = urljoin(self.base_url, url)
        # kwargs.setdefault('headers', {})
        # kwargs['headers']['Accept'] = 'application/json, text/javascript, */*; q=0.01'
        return super(Es, self).request(method, url, **kwargs)


es = Es()


@click.group()
@click.option('--base-url', envvar='ES', default=Es.base_url)
def cli(base_url):
    es.base_url = base_url


@cli.command()
@click.argument('text')
def analyze(text):
    for index, analyzers in sorted(get_analyzers_per_index().items()):
        for analyzer in sorted(analyzers):
            analyzer_name = '%s.%s' % (index, analyzer)
            tokens = es.get('/%s/_analyze' % index, params={
                'analyzer': analyzer,
                'text': text,
            }).json()['tokens']
            click.secho(analyzer_name, bold=True)
            click.secho('-' * len(analyzer_name), bold=True)
            styles = [
                {'fg': 'red'},
                {'fg': 'green'},
                {'fg': 'blue'},
            ]
            for token, style in zip(tokens, cycle(styles)):
                click.secho('%s ' % token['token'], nl=False, **style)
            click.echo('\n')

    pass


def get_analyzers_per_index():
    analyzers_per_index = defaultdict(set)
    for index, data in es.get('/_cluster/state').json()['metadata']['indices'].items():
        for mapping_data in data.get('mappings', {}).values():
            for property_data in mapping_data.get('properties', {}).values():
                analyzer = property_data.get('analyzer')
                if analyzer:
                    analyzers_per_index[index].add(analyzer)
    return analyzers_per_index


if __name__ == '__main__':
    cli()
