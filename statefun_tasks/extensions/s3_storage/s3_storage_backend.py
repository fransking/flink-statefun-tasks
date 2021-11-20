from statefun_tasks import StorageBackend, TasksException
from aiobotocore.session import get_session
from typing import List
import asyncio


class S3StorageBackend(StorageBackend):
    """
    An example S3 based storage backend
    :param bucket: S3 bucket name
    :param endpoint_url: endpoint url if using S3 compatible storage
    :param region_name: AWS S3 region name
    :param aws_secret_access_key: secret access key
    :param aws_access_key_id: access key id
    :param optional threshold: threshold in bytes at which this storage backend will begin offloading from pipeline state into S3
    """
    def __init__(self, bucket, endpoint_url=None, region_name=None, aws_secret_access_key=None, aws_access_key_id=None, threshold=1024000):
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_access_key_id = aws_access_key_id
        self._client = None
        self._threshold = threshold

    @property
    def threshold(self) -> int:
        return self._threshold

    async def __aenter__(self):
        session = get_session()

        self._client = await session.create_client('s3', 
            endpoint_url=self._endpoint_url,
            region_name=self._region_name, 
            aws_secret_access_key=self._aws_secret_access_key,
            aws_access_key_id=self._aws_access_key_id).__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    async def store(self, keys: List[str], value: bytes):
        if self._client is None:
            raise TasksException('S3StorageBackend should be used with async with')

        await self._client.put_object(Bucket=self._bucket, Key='/'.join(keys), Body=value)

    async def fetch(self, keys: List[str]) -> bytes:
        if self._client is None:
            raise TasksException('S3StorageBackend should be used with async with')

        response = await self._client.get_object(Bucket=self._bucket, Key='/'.join(keys))
        async with response['Body'] as stream:
            return await stream.read()

    async def delete(self, keys: List[str]) -> bytes:
        if self._client is None:
            raise TasksException('S3StorageBackend should be used with async with')

        keys_to_delete = []

        paginator = self._client.get_paginator('list_objects')
        async for result in paginator.paginate(Bucket=self._bucket, Prefix='/'.join(keys)):
            keys_to_delete.extend([obj['Key'] for obj in result.get('Contents', [])])

        deletes = [asyncio.ensure_future(self._client.delete_object(Bucket=self._bucket, Key=key)) for key in keys_to_delete]

        if any(deletes):
            await asyncio.gather(*deletes, return_exceptions=True)
