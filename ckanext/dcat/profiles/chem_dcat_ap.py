import requests
import json
from decimal import Decimal, DecimalException
from rdflib import term, URIRef, BNode, Literal, Graph
import ckantoolkit as toolkit
from .dcat_4c_ap import (Agent,
                        Dataset,
                        DataGeneratingActivity,
                        DefinedTerm,
                        Document,
                        EvaluatedEntity,
                        Entity,
                        Standard,
                        QualitativeAttribute,
                        QuantitativeAttribute)
from .dcat_4c_ap import (SubstanceSample,
SubstanceSampleCharacterizationDataset,
SubstanceSampleCharacterization,
InChi, InChIKey, IUPACName, SMILES, MolecularFormula, MolarMass, ChemicalEntity)

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
    CHEMINF,
    CHMO,
    OBI,
    IAO,
    PROV,
    CHEBI,
    NMR,
    QUDT,
    NCIT,
    FIX,
    namespaces,
)

from linkml_runtime.dumpers import RDFLibDumper
from linkml_runtime.utils.schemaview import SchemaView
import yaml

from . import EuropeanDCATAPProfile, EuropeanDCATAP2Profile

from rdflib.namespace import Namespace, RDF, XSD, SKOS, RDFS

import logging
log = logging.getLogger(__name__)


class ChemDCATAPProfile(EuropeanDCATAPProfile):
    def parse_dataset(self, dataset_dict, dataset_ref):
        log.debug("parsing dataset for chem dcat ap")
        dataset_dict["title"] = str(dataset_ref.value(DCT.title) or "")
        dataset_dict["notes"] = str(dataset_ref.value(DCT.description) or "")
        dataset_dict["doi"] = str(dataset_ref.value(DCT.identifier) or "")
        dataset_dict["language"] = str(dataset_ref.value(DCT.language) or "")
        return dataset_dict


    def _dataset_identity(self, dataset_dict):
        if dataset_dict.get("doi"):
            dataset_uri = "https://doi.org/" + dataset_dict.get("doi")
            dataset_id = dataset_uri
        else:
            dataset_uri = dataset_dict.get("id").strip()
            dataset_id = dataset_uri
        return dataset_uri, dataset_id


    def _normalize_language_code(self, raw_lang):
        raw_lang = (raw_lang or "").strip().lower()
        if raw_lang in ("english", "en", "en-us", "en-gb", "eng"):
            return "en"
        elif raw_lang in ("deutsch", "german", "de"):
            return "de"
        elif raw_lang:
            return raw_lang
        else:
            return "en"


    def _creator_agents(self, dataset_dict):
        creators = []
        try:
            if dataset_dict.get("author"):
                for creator in dataset_dict.get("author").replace("., ", ".|").split("|"):
                    creator = creator.strip()
                    if creator:
                        creators.append(Agent(name=creator))
            else:
                creators.append(Agent(name="NA"))
        except Exception as e:

            log.error(e)
        return creators



    def _get_pubchem_cid(self,inchi_key=None, smiles=None):
        key = inchi_key or smiles
        _pubchem_cache = {}
        if key in _pubchem_cache:
            return _pubchem_cache[key]

        try:
            if inchi_key:
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/{inchi_key}/cids/TXT"
            elif smiles:
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/smiles/{smiles}/cids/TXT"
            else:
                return None

            r = requests.get(url, timeout=5)

            if r.status_code == 200:
                cid = r.text.strip().split("\n")[0]
                _pubchem_cache[key] = cid
                return cid

        except Exception:
            return None

        _pubchem_cache[key] = None
        return None

# TODO: Think about which namespaces shouldbe passed to the RDFLibDumper as prefix_map for those prefixes that are
#  not already part of the DCAT-AP schema YAMLs. Should probably just be a Python dict maintained in this profile.

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        for prefix, namespace in namespaces.items():
            self.g.bind(prefix, namespace)

        CHEMDCATAP = Namespace("https://w3id.org/nfdi-de/dcat-ap-plus/chemistry/")
        self.g.bind("chemdcatap", CHEMDCATAP)

        dataset_uri, dataset_id = self._dataset_identity(dataset_dict)

        # -------------------------
        # Compound
        # -------------------------
        inchi_key = dataset_dict.get("inchi_key")
        smiles = dataset_dict.get("smiles")

        cid = self._get_pubchem_cid(inchi_key= inchi_key, smiles=smiles)

        if cid:
            compound_id = f"https://pubchem.ncbi.nlm.nih.gov/compound/{cid}"
        else:
            compound_id = f"{dataset_id}#sample_compound"

        compound_kwargs = {
            "id": compound_id,
        }

        if dataset_dict.get("inchi_key"):
            compound_kwargs["inchikey"] = InChIKey(
                title="assigned InChIKey",
                value=dataset_dict.get("inchi_key")
            )

        if dataset_dict.get("inchi"):
            compound_kwargs["inchi"] = InChi(
                title="assigned InChI",
                value=dataset_dict.get("inchi")
            )

        if dataset_dict.get("smiles"):
            compound_kwargs["smiles"] = SMILES(
                title="assigned SMILES",
                value=dataset_dict.get("smiles")
            )

        if dataset_dict.get("mol_formula"):
            compound_kwargs["molecular_formula"] = MolecularFormula(
                title="assigned IUPAC chemical formula",
                value=dataset_dict.get("mol_formula")
            )

        if dataset_dict.get("exactmass"):
            compound_kwargs["has_molar_mass"] = MolarMass(
                has_quantity_type="http://qudt.org/vocab/quantitykind/MolarMass",
                unit="https://qudt.org/vocab/unit/GM-PER-MOL",
                title="assigned exact mass",
                value=dataset_dict.get("exactmass")
            )

        if dataset_dict.get("iupacName"):
            compound_kwargs["iupac_name"] = IUPACName(
                title="assigned IUPAC name",
                value=dataset_dict.get("iupacName")
            )

        compound_chem = ChemicalEntity(**compound_kwargs)

        # -------------------------
        # Sample
        # -------------------------
        sample_chem = SubstanceSample(
            id=f"{dataset_id}#sample",
            title="evaluated sample",
            composed_of=[compound_chem.id]
        )

        # -------------------------
        # Measurement
        # -------------------------
        technique_iri = dataset_dict.get("measurement_technique_iri") or "http://purl.obolibrary.org/obo/OBI_0000070"
        technique_label = dataset_dict.get("measurement_technique") or "assay"

        measurement_chem = SubstanceSampleCharacterization(
            id=f"{dataset_id}#measurement",
            rdf_type=DefinedTerm(
                id=technique_iri,
                title=technique_label
            ),
            evaluated_entity=[sample_chem.id]
        )

        # -------------------------
        # Dataset
        # -------------------------
        dataset_chem = SubstanceSampleCharacterizationDataset(
            id=dataset_uri,
            title=dataset_dict.get("title"),
            description=dataset_dict.get("notes") or "No description",
            was_generated_by=[measurement_chem.id],
            identifier=dataset_id,
            is_about_entity=[sample_chem.id]
        )

        sv_chem_dcat_ap = SchemaView(
            "/usr/lib/ckan/default/src/ckanext-dcat/ckanext/dcat/schemas/chem_dcat_ap.yaml",
            merge_imports=True
        )

        rdf_nfdi_dumper = RDFLibDumper()

        prefix_map = {'@base': 'https://search.nfdi4chem.de/dataset/',
                      'CHEMINF': 'http://semanticscience.org/resource/CHEMINF_',
                      'CHMO': 'http://purl.obolibrary.org/obo/CHMO_',
                      'CHEBI': 'http://purl.obolibrary.org/obo/CHEBI_'
                      }

        try:
            nfdi_graph = rdf_nfdi_dumper.as_rdf_graph(dataset_chem, schemaview=sv_chem_dcat_ap, prefix_map = prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(sample_chem, schemaview=sv_chem_dcat_ap, prefix_map = prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(compound_chem, schemaview=sv_chem_dcat_ap, prefix_map = prefix_map)
            nfdi_graph += rdf_nfdi_dumper.as_rdf_graph(measurement_chem, schemaview=sv_chem_dcat_ap, prefix_map = prefix_map)
        except Exception as e:
            log.warning("ChemDCAT-AP serialization skipped: %s", e)
            return None

        for triple in nfdi_graph:
            self.g.add(triple)

        # -------------------------
        # Important explicit triples
        # -------------------------
        dataset_node = URIRef(dataset_uri)
        sample_node = URIRef(f"{dataset_id}#sample")
        compound_node = URIRef(f"{dataset_id}#sample_compound")
        measurement_node = URIRef(f"{dataset_id}#measurement")

        # explicit profile typing
        self.g.add((dataset_node, RDF.type, CHEMDCATAP.SubstanceSampleCharacterizationDataset))

        # explicit sample/compound relation in case dumper misses it
        #self.g.add((sample_node, URIRef("http://purl.obolibrary.org/obo/BFO_0000051"), compound_node))

        # keep DCAT relation
        self.g.add((dataset_node, PROV.wasGeneratedBy, measurement_node))

        # -------------------------
        # Language
        # -------------------------
        code = self._normalize_language_code(dataset_dict.get("language"))
        lang_uri = URIRef(f"http://id.loc.gov/vocabulary/iso639-1/{code}")
        self.g.add((lang_uri, RDF.type, DCT.LinguisticSystem))
        self.g.add((dataset_node, DCT.language, lang_uri))

        # -------------------------
        # Publisher
        # -------------------------
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

        # -------------------------
        # Standard
        # -------------------------
        std_b = BNode()
        self.g.add((dataset_node, DCT.conformsTo, std_b))
        self.g.add((std_b, RDF.type, DCT.Standard))
        self.g.add((
            std_b,
            DCT.identifier,
            URIRef("https://docs.nmrxiv.org/submission-guides/data-model/spectra.html"),
        ))

