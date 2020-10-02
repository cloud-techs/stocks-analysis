from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3


region = "us-east-1"
service = "es"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)



def es_load_data(index,body,id):
    end_point = 'https://search-techanalysis-hsbla7m4ghvqpymfmyfry4i3wu.us-east-1.es.amazonaws.com'
    es = Elasticsearch(
        hosts = [end_point],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    print(es.create(index=index, id=id, body=body))


def es_update_document(index, body, id):
    end_point = 'https://search-techanalysis-hsbla7m4ghvqpymfmyfry4i3wu.us-east-1.es.amazonaws.com'
    es = Elasticsearch(
        hosts = [end_point],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    print(es.index(index=index, id=id, body=body))

def es_get_doc_source(index, id):
    try:
        end_point = 'https://search-techanalysis-hsbla7m4ghvqpymfmyfry4i3wu.us-east-1.es.amazonaws.com'
        es = Elasticsearch(
            hosts = [end_point],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )
        data = es.get_source(index=index, id=id)
        return data
    except:
        return None


