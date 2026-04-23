import json
from decimal import Decimal, DecimalException

from rdflib import term, URIRef, BNode, Literal, Graph
import ckantoolkit as toolkit

# from ckan.lib.munge import munge_tag
import logging

from ckanext.dcat.profiles.dcat_4c_ap import (Agent,
                                              AnalysisDataset,
                                              AnalysisSourceData,
                                              DataAnalysis,
                                              Activity as DataCreatingActivity,
                                              DefinedTerm,
                                              Document,
                                              EvaluatedEntity,
                                              LinguisticSystem,
                                              Standard,
                                              QualitativeAttribute)
from . import EuropeanDCATAPProfile, EuropeanDCATAP2Profile

log = logging.getLogger(__name__)

from ckanext.dcat.utils import (
    resource_uri,
    DCAT_EXPOSE_SUBCATALOGS,
    DCAT_CLEAN_TAGS,
    publisher_uri_organization_fallback,
)
from .base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from .base import (
    RDF,
    XSD,
    SKOS,
    RDFS,
    DCAT,
    DCT,
    ADMS,
    VCARD,
    FOAF,
    SCHEMA,
    NFDI,
    CHEMINF,  # this
    CHMO,  # this
    OBI,
    IAO,
    PROV,
    CHEBI,
    NMR,
    QUDT,
    NCIT,
    FIX,
    namespaces
)
from linkml_runtime.dumpers import RDFLibDumper
from linkml_runtime.utils.schemaview import SchemaView


class DCATNFDi4ChemProfile(EuropeanDCATAPProfile):
    """
    An RDF profile extending DCAT-AP for NFDI4Chem

    Extends the EuropeanDCATAPProfile to support NFDI4Chem-specific fields.
    """

    def parse_dataset(self, dataset_dict, dataset_ref):
        # TODO: Create a parser
        log.debug('parsing dataset for test ')
        dataset_dict['title'] = str(dataset_ref.value(DCT.title))
        dataset_dict['notes'] = str(dataset_ref.value(DCT.description))
        dataset_dict['doi'] = str(dataset_ref.value(DCT.identifier))
        dataset_dict['language'] = [
            str(theme.value(SKOS.prefLabel)) for theme in dataset_ref.objects(DCAT.theme)
        ]
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        for prefix, namespace in namespaces.items():
            self.g.bind(prefix, namespace)

        # --- dataset URI (single canonical URI = DOI if available) ---
        if dataset_dict.get('doi'):
            dataset_uri = 'https://doi.org/' + dataset_dict.get('doi')
            dataset_id = dataset_uri
        else:
            dataset_uri = dataset_dict.get('id').strip()
            dataset_id = dataset_uri

        # --- sample ---
        # --- sample ---
        sample = EvaluatedEntity(
            id=dataset_id + '#sample',
            title='evaluated sample',
            has_qualitative_attribute=[
                QualitativeAttribute(
                    rdf_type=DefinedTerm(id='CHEMINF:000059', title='InChiKey'),
                    title='assigned InChiKey',
                    value=dataset_dict.get('inchi_key') or "not available"
                ),
                QualitativeAttribute(
                    rdf_type=DefinedTerm(id='CHEMINF:000113', title='InChi'),
                    title='assigned InChi',
                    value=dataset_dict.get('inchi')
                ),
                QualitativeAttribute(
                    rdf_type=DefinedTerm(id='CHEMINF:000018', title='SMILES'),
                    title='assigned SMILES',
                    value=dataset_dict.get('smiles')
                ),
                QualitativeAttribute(
                    rdf_type=DefinedTerm(id='CHEMINF:000037', title='IUPACChemicalFormula'),
                    title='assigned IUPACChemicalFormula',
                    value=dataset_dict.get('mol_formula') or "not available"
                )
            ]
        )

        # --- measurement ---
        measurement = None
        if dataset_dict.get('measurement_technique_iri'):
            measurement = DataCreatingActivity(
                id=f"{dataset_id}#measurement",
                rdf_type=DefinedTerm(
                    id=dataset_dict['measurement_technique_iri'],
                    title=dataset_dict.get('measurement_technique')
                )
            )

        # --- spectrum ---
        spectrum_kwargs = dict(
            id=f"{dataset_id}#spectrum",
            rdf_type=DefinedTerm(id='CHMO:0000800', title='spectrum')
        )
        if measurement is not None:
            spectrum_kwargs['was_generated_by'] = measurement
        spectrum = AnalysisSourceData(**spectrum_kwargs)

        # --- analysis ---
        analysis = DataAnalysis(
            id=f"{dataset_id}#analysis",
            rdf_type=DefinedTerm(
                id='http://purl.allotrope.org/ontologies/process#AFP_0003618',
                title='peak identification'
            ),
            evaluated_entity=[spectrum]
        )

        # --- dataset ---
        dataset = AnalysisDataset(
            id=dataset_uri,
            title=dataset_dict.get('title'),
            description=dataset_dict.get('notes') or 'No description',
            was_generated_by=analysis,
            identifier=dataset_id,
            is_about_entity=sample,
            conforms_to=Standard(
                id='https://docs.nmrxiv.org/submission-guides/data-model/spectra.html'
            )
        )

        # --- creators ---
        creators = []
        try:
            if dataset_dict.get('author'):
                for creator in dataset_dict.get('author').replace('., ', '.|').split('|'):
                    creators.append(Agent(name=creator))
            else:
                creators.append(Agent(name='NA'))
            dataset.creator = creators
        except Exception as e:
            log.error(e)

        # --- language normalization ---
        raw_lang = (dataset_dict.get('language') or '').strip().lower()
        if raw_lang in ('english', 'en', 'en-us', 'en-gb', 'eng'):
            code = 'en'
        elif raw_lang in ('deutsch', 'german', 'de'):
            code = 'de'
        elif raw_lang:
            code = raw_lang
        else:
            code = 'en'

        # --- landing page ---
        if dataset_dict.get('url'):
            dataset.landing_page = Document(id=dataset_dict.get('url'))

        # --- dates ---
        dataset.release_date = dataset_dict.get('metadata_created')
        dataset.modification_date = dataset_dict.get('metadata_modified')

        schemaview = SchemaView(schema="/usr/lib/ckan/default/src/ckanext-dcat/ckanext/dcat/schemas/dcat_4c_ap.yaml")
        rdf_nfdi_dumper = RDFLibDumper()

        prefix_map = {'@base': 'https://search.nfdi4chem.de/dataset/',
                      'CHEMINF': 'http://semanticscience.org/resource/CHEMINF_',
                      'CHMO': 'http://purl.obolibrary.org/obo/CHMO_',
                      'CHEBI': 'http://purl.obolibrary.org/obo/CHEBI_'
                      }

        try:
            nfdi_graph = rdf_nfdi_dumper.as_rdf_graph(dataset, schemaview=schemaview, prefix_map=prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(sample, schemaview=schemaview, prefix_map = prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(analysis, schemaview=schemaview, prefix_map=prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(spectrum, schemaview=schemaview, prefix_map=prefix_map )
            if measurement is not None:
                nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(measurement, schemaview=schemaview, prefix_map=prefix_map)
        except Exception as e:
            log.warning("DCAT-AP-PLUS serialization skipped: %s", e)
            return None

        from rdflib import URIRef, BNode, Literal

        dataset_node = URIRef(dataset_uri)

        sample_uri = URIRef(f"{dataset_id}#sample")
        sample_compound_uri = URIRef(f"{dataset_id}#sample_compound")

        self.g.add((sample_uri, RDF.type, URIRef("http://purl.obolibrary.org/obo/CHEBI_59999")))
        self.g.add((sample_uri, RDF.type, PROV.Entity))
        self.g.add((sample_uri, DCT.hasPart, sample_compound_uri))
        self.g.add((sample_uri, DCT.title, Literal("evaluated sample")))

        lang_uri = URIRef(f"http://id.loc.gov/vocabulary/iso639-1/{code}")
        std_b = BNode()

        self.g.add((dataset_node, DCT.conformsTo, std_b))
        self.g.add((std_b, RDF.type, DCT.Standard))
        self.g.add((std_b, DCT.identifier,
                    URIRef('https://docs.nmrxiv.org/submission-guides/data-model/spectra.html')))

        self.g.add((lang_uri, RDF.type, DCT.LinguisticSystem))
        self.g.add((dataset_node, DCT.language, lang_uri))

        # --- publisher ---
        org = dataset_dict.get("organization") or {}
        org_id = org.get("id")
        org_name = org.get("title") or org.get("display_name") or org.get("name")
        org_homepage = org.get("url")

        if org_id:
            site_url = toolkit.config.get("ckan.site_url")
            org_uri = URIRef(f"{site_url}/organization/{org_id}")

            self.g.add((dataset_node, DCT.publisher, org_uri))
            self.g.add((org_uri, RDF.type, FOAF.Organization))
            if org_name:
                self.g.add((org_uri, FOAF.name, Literal(org_name)))
            if org_homepage:
                self.g.add((org_uri, FOAF.homepage, URIRef(org_homepage)))

        # --- add graph ---
        for triple in nfdi_graph:
            self.g.add(triple)



