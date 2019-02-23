"""
MDS Provider API client implementation. 
"""

from datetime import datetime
import mds
from mds.api.auth import OAuthClientCredentialsAuth
from mds.providers import Provider
from mds.version import Version, mds_version_supported
import time

class ProviderClient(OAuthClientCredentialsAuth):
    """
    Client for MDS Provider APIs
    """
    def __init__(self, **kwargs):
        """
        Initialize a new ProviderClient object.

        Supported keyword args:

        :provider: a Provider instance that this client uses by default.

        :version: the MDS version to target, e.g. `x.y.z`. Can be str or `mds.version.Version` instance. By default,
                  By default, target the minimum version of MDS supported by the current version of this library.
        """
        self.encoder = CustomJsonEncoder(date_format="unix")
        self.provider = kwargs.pop("provider", None)
        self.version = Version(kwargs.pop("version", Version.MDS()))

        if not mds_version_supported(self.version):
            raise ValueError(f"MDS version {self.version} is not supported by the current version of this library.")

    def _media_type_version_header(self):
        """
        The custom MDS media-type and version header, using this client's version
        """
        version = f"{self.version.tuple[0]}.{self.version.tuple[1]}"
        return f"application/vnd.mds.provider+json;version={version}"

    def _session(self, provider):
        """
        Establish an authenticated session for a provider.
        """
        if hasattr(provider, "token") and not hasattr(provider, "token_url"):
            # auth token defined by provider
            session = self.auth_token_session(provider)
        else:
            # OAuth 2.0 client_credentials grant flow
            session = self.oauth_session(provider)

        session.headers.update({ "Accept": f"{self._media_type_version_header()}" })

        return session

    def _build_url(self, provider, endpoint):
        """
        Build an API url for a provider's endpoint.
        """
        url = provider.mds_api_url

        if hasattr(provider, "mds_api_suffix"):
            url += "/" + getattr(provider, "mds_api_suffix").rstrip("/")

        url += "/" + endpoint

        return url

    def _request(self, provider, endpoint, params, paging, rate_limit):
        """
        Send one or more requests to a provider's endpoint.

        Returns a list of payloads, with length corresponding to the number of non-empty responses.
        """
        def __describe(res):
            """
            Prints details about the given response.
            """
            print(f"Requested {res.url}, Response Code: {res.status_code}")
            print("Response Headers:")
            for k,v in res.headers.items():
                print(f"{k}: {v}")

            if r.status_code is not 200:
                print(r.text)

        def __has_data(page):
            """
            Checks if this page has a "data" property with a non-empty payload.
            """
            data = page["data"] if "data" in page else {"__payload__": []}
            payload = data[endpoint] if endpoint in data else []
            print(f"Got payload with {len(payload)} {endpoint}")
            return len(payload) > 0

        def __next_url(page):
            """
            Gets the next URL or None from page.
            """
            return page["links"].get("next") if "links" in page else None

        url = self._build_url(provider, endpoint)
        results = []

        # establish an authenticated session
        session = self._session(provider)

        # get the initial page of data
        r = session.get(url, params=params)

        if r.status_code is not 200:
            __describe(r)
            return results

        this_page = r.json()

        if __has_data(this_page):
            results.append(this_page)

        # get subsequent pages of data
        next_url = __next_url(this_page)
        while paging and next_url:
            r = session.get(next_url)

            if r.status_code is not 200:
                __describe(r)
                break

            this_page = r.json()

            if __has_data(this_page):
                results.append(this_page)

            next_url = __next_url(this_page)

            if next_url and rate_limit:
                time.sleep(rate_limit)

        return results

    def _date_format(self, dt):
        """
        Format datetimes for querystrings.
        """
        if dt is None:
            return None
        return self.encoder.encode(dt) if isinstance(dt, datetime) else int(dt)

    def _provider_or_raise(self, **kwargs):
        """
        Get a Provider instance from kwargs, self, or raise an error.
        """
        provider = kwargs.pop("provider", None) or self.provider
        if provider is None:
            raise ValueError("Provider instance not found for ProviderClient")

        return provider

    def get_status_changes(self, **kwargs):
        """
        Request status changes, returning a list of non-empty payloads.

        Supported keyword args:

        :provider: Provider to issue this request to.
                   By default issue the request to this client's Provider instance.

        :start_time: Filters for status changes where event_time occurs at or after the given time
                     Should be a datetime object or int UNIX milliseconds

        :end_time: Filters for status changes where event_time occurs before the given time
                   Should be a datetime object or int UNIX milliseconds

        :paging: True (default) to follow paging and request all available data.
                 False to request only the first page.

        :rate_limit: Number of seconds of delay to insert between paging requests.
        """
        provider = self._provider_or_raise(**kwargs)
        start_time = self._date_format(kwargs.pop("start_time", None))
        end_time = self._date_format(kwargs.pop("end_time", None))
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))

        params = {
            **dict(start_time=start_time, end_time=end_time),
            **kwargs
        }

        return self._request(provider, mds.STATUS_CHANGES, params, paging, rate_limit)

    def get_trips(self, **kwargs):
        """
        Request trips, returning a list of non-empty payloads.

        Supported keyword args:

        :provider: Provider to issue this request to.
                   By default issue the request to this client's Provider instance.

        :device_id: Filters for trips taken by the given device.

        :vehicle_id: Filters for trips taken by the given vehicle.

        :min_end_time: Filters for trips where end_time occurs at or after the given time.
                       Should be a datetime object or int UNIX milliseconds

        :max_end_time: Filters for trips where end_time occurs before the given time.
                       Should be a datetime object or int UNIX milliseconds

        :paging: True (default) to follow paging and request all available data.
                 False to request only the first page.

        :rate_limit: Number of seconds of delay to insert between paging requests.
        """
        provider = self._provider_or_raise(**kwargs)
        min_end_time = self._date_format(kwargs.pop("min_end_time", None))
        max_end_time = self._date_format(kwargs.pop("max_end_time", None))
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))

        params = { 
            **dict(device_id=device_id, vehicle_id=vehicle_id, min_end_time=min_end_time, max_end_time=max_end_time),
            **kwargs
        }

        return self._request(provider, mds.TRIPS, params, paging, rate_limit)
