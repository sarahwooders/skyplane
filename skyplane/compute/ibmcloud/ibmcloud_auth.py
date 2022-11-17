from typing import Optional
from skyplane.config_paths import config_path, ibmcloud_config_path
from skyplane.config import SkyplaneConfig
from skyplane.utils import imports

PUBLIC_ENDPOINT = 'https://s3.{}.cloud-object-storage.appdomain.cloud'
PRIVATE_ENDPOINT = 'https://s3.private.{}.cloud-object-storage.appdomain.cloud'
DIRECT_ENDPOINT = 'https://s3.direct.{}.cloud-object-storage.appdomain.cloud'

OBJ_REQ_RETRIES = 5
CONN_READ_TIMEOUT = 10
VPC_API_VERSION = '2021-09-21'


class IBMCloudAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        """Loads IBM Cloud authentication details. If no access key is provided, it will try to load credentials using boto3"""
        if not config is None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)

        self.user_agent = self.config.ibmcloud_useragent if self.config.ibmcloud_useragent is not None else 'skyplane-ibm'

        self.config.ibmcloud_useragent
        if self.config.ibmcloud_access_id and self.config.ibmcloud_secret_key:
            self.config_mode = "manual"
            self._access_key = self.config.ibmcloud_access_id
            self._secret_key = self.config.ibmcloud_secret_key
        else:
            self.config_mode = "iam_inferred"
            self._access_key = None
            self._secret_key = None
            self.iam_key = self.config.ibmcloud_iam_key

    def __get_ibmcloud_endpoint(self, region, compute_backend = 'public'):
        if (region is not None):
            endpoint = PUBLIC_ENDPOINT.format(region)

            if compute_backend == 'ibm_vpc':
                endpoint = DIRECT_ENDPOINT.format(region)
            
            return endpoint


    @imports.inject("ibm_cloud_sdk_core", "ibm_vpc", pip_extra="ibmcloud")
    def save_region_config(ibm_cloud_sdk_core, ibm_vpc, self, config: SkyplaneConfig):
        if not config.ibmcloud_enabled:
            self.clear_region_config()
            return
        with ibmcloud_config_path.open("w") as f:
            region_list = []
            authenticator = ibm_cloud_sdk_core.authenticators.IAMAuthenticator(config.ibmcloud_iam_key,
                url=config.ibmcloud_iam_endpoint)
            ibm_vpc_client = ibm_vpc.VpcV1(VPC_API_VERSION, authenticator=authenticator)
            res = ibm_vpc_client.list_regions()
            for region in res.result['regions']:
                if region['status'] == 'available':
                    region_list.append(region['name'])
            f.write("\n".join(region_list))

    def clear_region_config(self):
        with ibmcloud_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        try:
            f = open(ibmcloud_config_path, "r")
        except FileNotFoundError:
            return []
        region_list = []
        for region in f.read().split("\n"):
            region_list.append(region)
        return region_list

    @property
    def access_key(self):
        if self._access_key is None:
            self._access_key, self._secret_key = self.infer_credentials()
        return self._access_key

    @property
    def secret_key(self):
        if self._secret_key is None:
            self._access_key, self._secret_key = self.infer_credentials()
        return self._secret_key

    def enabled(self):
        return self.config.ibmcloud_enabled

    @imports.inject("ibm_boto3", pip_extra="aws")
    def infer_credentials(ibm_boto3, self):
        # todo load temporary credentials from STS
        cached_credential = getattr(self.__cached_credentials, "ibm_boto3_credential", None)
        if cached_credential is None:
            session = ibm_boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                credentials = credentials.get_frozen_credentials()
                cached_credential = (credentials.access_key, credentials.secret_key)
            setattr(self.__cached_credentials, "ibm_boto3_credential", cached_credential)
        return cached_credential if cached_credential else (None, None)

    @imports.inject("ibm_boto3", pip_extra="ibmcloud")
    def get_boto3_session(ibm_boto3, self, cos_region: Optional[str] = None):
        if self.config_mode == "manual":
            return ibm_boto3.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
        else:
            return None

    def get_boto3_resource(self, service_name, cos_region=None):
        return self.get_boto3_session().resource(service_name, region_name=cos_region)

    def get_region(self):
        return self.config.ibmcloud_region

    @imports.inject("ibm_boto3", "ibm_botocore", pip_extra="ibmcloud")
    def get_boto3_client(ibm_boto3, ibm_botocore, self, service_name, cos_region=None):
        client_config = ibm_botocore.client.Config(max_pool_connections=128,
                                                       user_agent_extra=self.user_agent,
                                                       connect_timeout=CONN_READ_TIMEOUT,
                                                       read_timeout=CONN_READ_TIMEOUT,
                                                       retries={'max_attempts': OBJ_REQ_RETRIES})

        if cos_region is None:
            return self.get_boto3_session().client(service_name, config=client_config)
        else:
            return self.get_boto3_session().client(service_name,
                endpoint_url = self.__get_ibmcloud_endpoint(cos_region),
                config = client_config)
