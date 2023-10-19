# Import stuff
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_tag_urn,
    make_term_urn,
    make_data_job_urn,
)
from datahub.api.entities.datajob.datajob import DataJob
from datahub.api.entities.datajob.dataflow import DataFlow
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.cli import delete_cli
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DateTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    NumberTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    TagPropertiesClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    DataJobInputOutputClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    EditableDatasetPropertiesClass,
    DatasetPropertiesClass,
    EdgeClass,
)
from datetime import datetime


def audit_stamp():
    return AuditStampClass(time=int(round(datetime.now().timestamp() * 1000)), actor="urn:li:corpuser:ingestion")


def make_dataset_class_list_into_urn_list(datasets: list):
    urn_list = []
    if len(datasets) == 0:
        raise Exception("Empty list of datasets passed")

    for dataset in datasets:
        if isinstance(dataset, DatasetClass):
            urn_list.append(dataset.datasetURN)
        else:
            raise Exception("make_dataset_class_list_into_urn_list expects lists of dataSetClass in datasets")

    return urn_list


def make_job_class_list_into_urn_list(jobs: list):
    urn_list = []
    if len(jobs) == 0:
        raise Exception("Empty list of jobs passed")

    for job in jobs:
        if isinstance(job, JobClass):
            urn_list.append(job.job_urn)
        else:
            raise Exception("make_job_class_list_into_urn_list expects lists of JobClass in jobs")

    return urn_list


def trap_emit_error(self, Exception):
    if Exception != "success":
        print(f"Error when emitting: {Exception}")




class TermClass:
    def __init__(self, gms_server_url, name, definition="", source=""):
        self.term_urn = make_term_urn(name)
        self.gms_server_url = gms_server_url
        self.term_properties = GlossaryTermInfoClass(
            definition=definition,
            name=name,
            termSource=source,
        )
        self.datahub_emitter = DatahubRestEmitter(gms_server=gms_server_url)
        return

    def fetch_term_from_server(self):
        graph = DataHubGraph(DatahubClientConfig(server=self.gms_server_url))
        self.term_properties = graph.get_aspects_for_entity(entity_urn=self.term_urn,
                                                            aspects=["glossaryTermInfo"],
                                                            aspect_types=[GlossaryTermInfoClass])["glossaryTermInfo"]
        if self.term_properties is not None:
            return True
        else:
            return False

    def make_emitter(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.term_urn,
            aspect=self.term_properties,
        )

    def emit(self):
        self.datahub_emitter.emit(item=self.make_emitter(), callback=trap_emit_error)
        return self

    def delete(self):
        delete_cli._delete_one_urn(urn=self.term_urn, soft=False, cached_emitter=self.datahub_emitter)
        return


class TagClass:
    def __init__(self, gms_server_url, name, description):
        self.tag_urn = make_tag_urn(name)
        self.tag_properties = TagPropertiesClass(name, description)
        self.datahub_emitter = DatahubRestEmitter(gms_server=gms_server_url)
        return

    def make_emitter(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.tag_urn,
            aspect=self.tag_properties,
        )

    def emit(self):
        self.datahub_emitter.emit(item=self.make_emitter(), callback=trap_emit_error)
        return self

    def delete(self):
        delete_cli._delete_one_urn(urn=self.tag_urn, soft=False, cached_emitter=self.datahub_emitter)
        return


class DatasetClass:
    def __init__(self, gms_server_url: str
                 , platform: str
                 , dataset_name: str
                 , dbname: str = None
                 , description: str = ""
                 , url: str = ""):
        self.datahub_emitter = DatahubRestEmitter(gms_server=gms_server_url)
        self.gms_server_url = gms_server_url
        self.platform = platform
        if dbname is None:
            self.full_dataset_name = dataset_name
        else:
            self.full_dataset_name = dbname + "." + dataset_name
        self.fields = []
        self.datasetURN = make_dataset_urn(platform=self.platform, name=self.full_dataset_name, env="PROD")
        self.description = description
        self.url = url
        self.data_set_level_term = None

        return

    def __str__(self):
        return f"{self.__class__} datasetname: {self.full_dataset_name}, platform: {self.platform}, description: {self.description}, url: {self.url}"

    def add_field(self, field_name: str, description: str, datatype: str, tags: str = None, terms: str = None):
        if datatype.lower() == "integer":
            data_type = SchemaFieldDataTypeClass(type=NumberTypeClass())
            native_type = "int64"
        elif datatype.lower() == "number":
            data_type = SchemaFieldDataTypeClass(type=NumberTypeClass())
            native_type = "DECIMAL(38,10)"
        elif datatype.lower() == "string":
            data_type = SchemaFieldDataTypeClass(type=StringTypeClass())
            native_type = "VARCHAR(100)"
        elif datatype.lower() == "date":
            data_type = SchemaFieldDataTypeClass(type=DateTypeClass())
            native_type = "Date"
        elif datatype.lower() == "double":
            data_type = SchemaFieldDataTypeClass(type=NumberTypeClass())
            native_type = "number"
        else:
            raise Exception(f"Datatype {datatype} is not valid for easydatahub, possible values are integer, number, string & date")

        if tags is not None and tags != "":
            tag_list = []
            for tag in tags.split("!"):
                tag_list.append(TagAssociationClass(make_tag_urn(tag)))
            global_tags = GlobalTagsClass(tags=tag_list)
        else:
            global_tags = None

        if terms is not None and terms != "":
            term_list = []
            for term in terms.split(";"):
                term_list.append(GlossaryTermAssociationClass(make_term_urn(term)))
            glossary_terms = GlossaryTermsClass(term_list, audit_stamp())
        else:
            glossary_terms = None

        self.fields.append(SchemaFieldClass(
            fieldPath=field_name,
            type=data_type,
            nativeDataType=native_type,  # use this to provide the type of the field in the source system's vernacular
            description=description,
            globalTags=global_tags,
            glossaryTerms=glossary_terms,
            lastModified=audit_stamp()
        ))
        return

    def get_description_from_server(self):
        return DataHubGraph(DatahubClientConfig(server=self.gms_server_url)).get_aspects_for_entity(
            entity_urn=self.datasetURN,
            aspects=["editableDatasetProperties"],
            aspect_types=[EditableDatasetPropertiesClass],
        )["editableDatasetProperties"].description

    def link_dataset_level_glossary_term(self, term: TermClass):
        self.data_set_level_term = GlossaryTermsClass(terms= [GlossaryTermAssociationClass(term.term_urn)], auditStamp=audit_stamp())

        return

    def __make_dataset_emitter__(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.datasetURN,
            aspect=SchemaMetadataClass(
                schemaName="customer",  # not used
                platform=make_data_platform_urn(self.platform),  # important <- platform must be an urn
                version=0,
                # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
                hash="",
                platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
                lastModified=audit_stamp(),
                fields=self.fields,
            ),
        )

    def __make_url_emitter__(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.datasetURN,
            aspect=InstitutionalMemoryClass(elements=[
                InstitutionalMemoryMetadataClass(url=self.url, description="Zendesk", createStamp=audit_stamp())])
        )

    def __make_doc_emitter__(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.datasetURN,
            aspect=EditableDatasetPropertiesClass(created=audit_stamp(),
                                                  description=self.description),
        )

    def __make_term_emitter__(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.datasetURN,
            aspect=self.data_set_level_term
        )

    def emit(self):
        self.datahub_emitter.emit(item=self.__make_dataset_emitter__(), callback=trap_emit_error)
        if self.url != "":
            self.datahub_emitter.emit(item=self.__make_url_emitter__(), callback=trap_emit_error)
        if self.description != "":
            self.datahub_emitter.emit(item=self.__make_doc_emitter__(), callback=trap_emit_error)
        if self.data_set_level_term is not None:
            self.datahub_emitter.emit(item=self.__make_term_emitter__(), callback=trap_emit_error)
        return self

    def delete(self):
        delete_cli._delete_one_urn(urn=self.datasetURN, soft=False, cached_emitter=self.datahub_emitter)
        return

class JobClass:
    datajob: DataJob

    def __init__(self
                 , gms_server_url: str
                 , orchestrator: str
                 , jobid: str
                 , job_name: str
                 , job_desc: str
                 , flow_id: str
                 , input_datasets: list = None
                 , output_datasets: list = None):
        self.job_urn = make_data_job_urn(orchestrator=orchestrator, flow_id=flow_id, job_id=jobid, cluster="PROD")
        self.datahub_emitter = DatahubRestEmitter(gms_server=gms_server_url)
        self.input_datasets = [] if input_datasets is None else input_datasets
        self.output_datasets = [] if output_datasets is None else output_datasets
        self.upstream_jobs = []
        self.datajob = DataJob(id=jobid,
                               flow_urn=DataFlow(id=flow_id, orchestrator=orchestrator, env="PROD").urn,
                               name=job_name,
                               description=job_desc,
                               )
        return

    def append_upstream_dataset(self, input_dataset):
        self.input_datasets.append(input_dataset)
        return

    def append_downstream_dataset(self, output_dataset):
        self.output_datasets.append(output_dataset)
        return

    def append_upstream_job(self, upstream_job):
        self.upstream_jobs.append(upstream_job)
        return

    def append_downstream_job(self, downstream_job):
        downstream_job.upstream_jobs.append(self)
        return

    def make_emitter(self):
        return MetadataChangeProposalWrapper(entityUrn=self.job_urn,
                                             aspect=DataJobInputOutputClass(
                                                 inputDatasets=make_dataset_class_list_into_urn_list(
                                                     self.input_datasets) if len(self.input_datasets) > 0 else [],
                                                 outputDatasets=make_dataset_class_list_into_urn_list(
                                                     self.output_datasets) if len(self.output_datasets) > 0 else [],
                                                 inputDatajobs=make_job_class_list_into_urn_list(
                                                     self.upstream_jobs) if len(self.upstream_jobs) > 0 else None,
                                             ), )

    def emit(self):
        # emit job itself
        self.datajob.emit(emitter=self.datahub_emitter, callback=trap_emit_error)
        self.datahub_emitter.emit(item=self.make_emitter(), callback=trap_emit_error)
        return self
