from utilities import CloudStorageClient,VirtualFileSystemClient

def test_blob():
    cloudClient = VirtualFileSystemClient(sas="TO_BE_REPLACED")
    cloudClient.save("success")
test_blob()