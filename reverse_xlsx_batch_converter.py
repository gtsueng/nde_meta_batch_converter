from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


TARGET_FILES = [
    "ACTG.json",
    "BV-BRC.json",
    "IEDB.json",
    "ITN TrialShare.json",
    "mwccs.json",
    "TBPortals.json",
    "ImmuneSpace.json",
]

RESOURCE_BASE_COLUMNS = [
    "name",
    "url",
    "identifier",
    "alternateName",
    "license",
    "License type",
    "conditionsOfAccess",
    "usageInfo",
    "abstract",
    "description",
    "citation pmid",
    "collectionType",
    "hasAPI",
    "hasDownload",
    "isAccessibleForFree",
    "inLanguage",
    "version",
    "genre",
    "dateModified",
    "dateCreated",
    "datePublished",
    "creativeWorkStatus",
]

ABOUT_COLUMNS = ["url", "type", "name", "url.1", "alternateName", "identifier"]
FUNDING_COLUMNS = [
    "url",
    "type",
    "identifier",
    "funder.@type",
    "funder.name",
    "funder.alternateName",
    "funder.parentOrganization",
]
COLLECTION_SIZE_COLUMNS = ["url", "minValue", "unitText"]
RELATED_COLUMNS = ["url", "property", "type", "name", "prop.url", "description", "pmid", "doi"]
AUTHOR_COLUMNS = [
    "url",
    "type",
    "name",
    "givenName",
    "familyName",
    "alternateName",
    "affiliation.name",
    "identifier",
    "parentOrganization",
]
DEFINED_TERMS_COLUMNS = ["url", "property", "name", "prop.url", "prop.description"]
DISTRIBUTION_COLUMNS = ["url", "type", "contentUrl", "encoding", "dateModified"]

DEFINED_TERM_PROPERTIES = {
    "about",
    "healthCondition",
    "infectiousAgent",
    "keywords",
    "measurementTechnique",
    "species",
    "topicCategory",
    "topicCoverage",
    "variableMeasured",
}

HANDLED_PROPERTIES = {
    "@context",
    "@type",
    "about",
    "abstract",
    "alternateName",
    "author",
    "collectionSize",
    "collectionType",
    "conditionsOfAccess",
    "creativeWorkStatus",
    "date",
    "dateCreated",
    "dateModified",
    "datePublished",
    "description",
    "distribution",
    "funding",
    "genre",
    "hasAPI",
    "hasDownload",
    "identifier",
    "inLanguage",
    "includedInDataCatalog",
    "isAccessibleForFree",
    "license",
    "name",
    "url",
    "usageInfo",
    "version",
}


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def clean_cell(value: Any) -> Any:
    if value in (None, "", [], {}):
        return None
    return value


def join_scalar_list(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, list):
        clean_values = [str(item).strip() for item in value if item not in (None, "")]
        return " | ".join(clean_values) or None
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value


def normalize_language(value: Any) -> Any:
    languages = []
    for item in ensure_list(value):
        if isinstance(item, dict):
            name = item.get("name")
            if name:
                languages.append(name)
        elif item not in (None, ""):
            languages.append(str(item))
    return " | ".join(dict.fromkeys(languages)) or None


def normalize_usage_info(value: Any) -> Any:
    entries = []
    for item in ensure_list(value):
        if isinstance(item, dict):
            entry = item.get("url") or item.get("name") or item.get("description")
            if entry:
                entries.append(entry)
        elif item not in (None, ""):
            entries.append(str(item))
    return " | ".join(dict.fromkeys(entries)) or None


def normalize_citation_pmids(value: Any) -> Any:
    pmids = []
    for item in ensure_list(value):
        if isinstance(item, dict) and item.get("pmid"):
            pmids.append(str(item["pmid"]))
    if not pmids:
        return None
    unique_pmids = list(dict.fromkeys(pmids))
    return "[" + ",".join(unique_pmids) + "]"


def infer_license_type(license_value: Any) -> Any:
    if not license_value:
        return None
    if isinstance(license_value, str) and license_value.startswith("http"):
        return "custom"
    return "standard"


def extract_affiliation_name(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("name")
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("name"):
                return item["name"]
    return None


def pack_extra_fields(record: dict[str, Any]) -> Any:
    skip_keys = {"@type", "name", "url", "contentUrl", "description", "pmid", "doi"}
    extras = {key: value for key, value in record.items() if key not in skip_keys and value not in (None, "", [], {})}
    if not extras:
        return None
    return json.dumps(extras, sort_keys=True)


def combine_description(description: Any, extra_payload: Any) -> Any:
    if description and extra_payload:
        return f"{description}\nEXTRA_JSON: {extra_payload}"
    if extra_payload:
        return f"EXTRA_JSON: {extra_payload}"
    return description


def build_resource_base_row(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": clean_cell(data.get("name")),
        "url": clean_cell(data.get("url")),
        "identifier": clean_cell(data.get("identifier")),
        "alternateName": clean_cell(join_scalar_list(data.get("alternateName"))),
        "license": clean_cell(data.get("license")),
        "License type": infer_license_type(data.get("license")),
        "conditionsOfAccess": clean_cell(data.get("conditionsOfAccess")),
        "usageInfo": clean_cell(normalize_usage_info(data.get("usageInfo"))),
        "abstract": clean_cell(data.get("abstract")),
        "description": clean_cell(data.get("description")),
        "citation pmid": clean_cell(normalize_citation_pmids(data.get("citation"))),
        "collectionType": clean_cell(data.get("collectionType")),
        "hasAPI": clean_cell(data.get("hasAPI")),
        "hasDownload": clean_cell(data.get("hasDownload")),
        "isAccessibleForFree": clean_cell(data.get("isAccessibleForFree")),
        "inLanguage": clean_cell(normalize_language(data.get("inLanguage"))),
        "version": clean_cell(data.get("version")),
        "genre": clean_cell(data.get("genre")),
        "dateModified": clean_cell(data.get("dateModified")),
        "dateCreated": clean_cell(data.get("dateCreated")),
        "datePublished": clean_cell(data.get("datePublished")),
        "creativeWorkStatus": clean_cell(data.get("creativeWorkStatus")),
    }


def append_about_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for item in ensure_list(data.get("about")):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "url": url,
                "type": item.get("@type", "DefinedTerm"),
                "name": clean_cell(item.get("name")),
                "url.1": clean_cell(item.get("url")),
                "alternateName": clean_cell(join_scalar_list(item.get("alternateName"))),
                "identifier": clean_cell(item.get("identifier")),
            }
        )


def append_funding_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for grant in ensure_list(data.get("funding")):
        if not isinstance(grant, dict):
            continue
        funders = ensure_list(grant.get("funder"))
        if not funders:
            funders = [{}]
        for funder in funders:
            funder = funder if isinstance(funder, dict) else {}
            rows.append(
                {
                    "url": url,
                    "type": grant.get("@type", "MonetaryGrant"),
                    "identifier": clean_cell(grant.get("identifier")),
                    "funder.@type": clean_cell(funder.get("@type", "Organization") if funder else None),
                    "funder.name": clean_cell(funder.get("name")),
                    "funder.alternateName": clean_cell(join_scalar_list(funder.get("alternateName"))),
                    "funder.parentOrganization": clean_cell(funder.get("parentOrganization")),
                }
            )


def append_collection_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for item in ensure_list(data.get("collectionSize")):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "url": url,
                "minValue": clean_cell(item.get("minValue")),
                "unitText": clean_cell(item.get("unitText")),
            }
        )


def append_author_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for item in ensure_list(data.get("author")):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "url": url,
                "type": clean_cell(item.get("@type")),
                "name": clean_cell(item.get("name")),
                "givenName": clean_cell(item.get("givenName")),
                "familyName": clean_cell(item.get("familyName")),
                "alternateName": clean_cell(join_scalar_list(item.get("alternateName"))),
                "affiliation.name": clean_cell(extract_affiliation_name(item.get("affiliation"))),
                "identifier": clean_cell(item.get("identifier")),
                "parentOrganization": clean_cell(item.get("parentOrganization")),
            }
        )


def append_defined_term_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for prop in sorted(DEFINED_TERM_PROPERTIES):
        for item in ensure_list(data.get(prop)):
            if isinstance(item, dict):
                rows.append(
                    {
                        "url": url,
                        "property": prop,
                        "name": clean_cell(item.get("name")),
                        "prop.url": clean_cell(item.get("url") or item.get("identifier")),
                        "prop.description": clean_cell(item.get("description")),
                    }
                )
            elif item not in (None, ""):
                rows.append(
                    {
                        "url": url,
                        "property": prop,
                        "name": None,
                        "prop.url": str(item),
                        "prop.description": None,
                    }
                )


def append_distribution_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for item in ensure_list(data.get("distribution")):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "url": url,
                "type": clean_cell(item.get("@type")),
                "contentUrl": clean_cell(item.get("contentUrl") or item.get("url")),
                "encoding": clean_cell(item.get("encoding")),
                "dateModified": clean_cell(item.get("dateModified")),
            }
        )


def append_related_rows(url: str, data: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for prop, value in data.items():
        if prop in HANDLED_PROPERTIES or prop in DEFINED_TERM_PROPERTIES:
            continue
        for item in ensure_list(value):
            if isinstance(item, dict):
                extra_payload = pack_extra_fields(item)
                rows.append(
                    {
                        "url": url,
                        "property": prop,
                        "type": clean_cell(item.get("@type")),
                        "name": clean_cell(item.get("name")),
                        "prop.url": clean_cell(item.get("url") or item.get("contentUrl")),
                        "description": clean_cell(combine_description(item.get("description"), extra_payload)),
                        "pmid": clean_cell(item.get("pmid")),
                        "doi": clean_cell(item.get("doi")),
                    }
                )
            elif item not in (None, ""):
                rows.append(
                    {
                        "url": url,
                        "property": prop,
                        "type": "URL",
                        "name": None,
                        "prop.url": str(item),
                        "description": None,
                        "pmid": None,
                        "doi": None,
                    }
                )


def build_workbook_frames(json_dir: Path, target_files: list[str]) -> dict[str, pd.DataFrame]:
    resource_base_rows: list[dict[str, Any]] = []
    about_rows: list[dict[str, Any]] = []
    funding_rows: list[dict[str, Any]] = []
    collection_rows: list[dict[str, Any]] = []
    related_rows: list[dict[str, Any]] = []
    author_rows: list[dict[str, Any]] = []
    defined_term_rows: list[dict[str, Any]] = []
    distribution_rows: list[dict[str, Any]] = []

    for filename in target_files:
        file_path = json_dir / filename
        data = json.loads(file_path.read_text(encoding="utf-8"))
        url = data["url"]
        resource_base_rows.append(build_resource_base_row(data))
        append_about_rows(url, data, about_rows)
        append_funding_rows(url, data, funding_rows)
        append_collection_rows(url, data, collection_rows)
        append_related_rows(url, data, related_rows)
        append_author_rows(url, data, author_rows)
        append_defined_term_rows(url, data, defined_term_rows)
        append_distribution_rows(url, data, distribution_rows)

    return {
        "resource_base": pd.DataFrame(resource_base_rows, columns=RESOURCE_BASE_COLUMNS),
        "about": pd.DataFrame(about_rows, columns=ABOUT_COLUMNS),
        "funding": pd.DataFrame(funding_rows, columns=FUNDING_COLUMNS),
        "collectionSize": pd.DataFrame(collection_rows, columns=COLLECTION_SIZE_COLUMNS),
        "related": pd.DataFrame(related_rows, columns=RELATED_COLUMNS),
        "author": pd.DataFrame(author_rows, columns=AUTHOR_COLUMNS),
        "definedTerms": pd.DataFrame(defined_term_rows, columns=DEFINED_TERMS_COLUMNS),
        "distribution": pd.DataFrame(distribution_rows, columns=DISTRIBUTION_COLUMNS),
    }


def convert_resource_catalogs(
    json_dir: Path | None = None,
    output_path: Path | None = None,
    target_files: list[str] | None = None,
) -> Path:
    script_path = Path.cwd()
    parent_path = script_path.parent
    json_dir = json_dir or parent_path / "nde-metadata-corrections" / "metadata_for_DDE" / "resourceCatalogs"
    target_files = target_files or TARGET_FILES
    output_path = output_path or script_path / "data" / f"{datetime.now():%Y_%m_%d}_RepoMetaCuration_reverse.xlsx"

    frames = build_workbook_frames(json_dir=json_dir, target_files=target_files)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, frame in frames.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)

    return output_path


if __name__ == "__main__":
    written_path = convert_resource_catalogs()
    print(f"Wrote {written_path}")
