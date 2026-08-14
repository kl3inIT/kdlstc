"""dlt-backed source adapter for the QL Gia REST API.

The warehouse owns the committed cursor.  This adapter receives that cursor
from Airflow and deliberately does not persist a second copy in dlt state.
It returns dlt ``PageData`` objects so the DAG can write the original HTTP
response bytes to Bronze before doing any normalization.
"""

from collections.abc import Iterator

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.client import PageData
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator


DLT_VERSION = dlt.__version__


def iter_price_pages(
    base_url: str,
    *,
    updated_since: str | None,
    page_size: int,
    timeout: int,
) -> Iterator[PageData[dict]]:
    """Yield QL Gia API pages using dlt's REST client and retry session.

    ``nextPage`` is an API pagination token, not the warehouse incremental
    cursor.  ``updatedSince`` always comes from ``ingestion.cursors`` and is
    only advanced by the DAG's atomic Gold publish transaction.
    """

    client = RESTClient(
        base_url=base_url.rstrip("/") + "/",
        headers={"Accept": "application/json"},
        data_selector="items",
        paginator=JSONResponseCursorPaginator(
            cursor_path="nextPage",
            cursor_param="page",
            stop_after_empty_page=True,
        ),
    )

    params = {
        "page": 1,
        "pageSize": page_size,
    }
    if updated_since is not None:
        params["updatedSince"] = updated_since

    yield from client.paginate(
        "api/prices",
        params=params,
        timeout=timeout,
    )
