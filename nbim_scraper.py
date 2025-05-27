import json
import os
from datetime import datetime
from typing import TypedDict

import pandas as pd
import requests
from parsel import Selector
from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet


class ScrapedItem(TypedDict):
    """A Type-safe representation of the scraped item. It's just a dict with autocomplete of the keys that exist in it"""
    id: str
    company: str
    decision: str
    publishing_date: str


class ChangesDict(TypedDict):
    """A Type-safe representation of the changes from one run to another. It's just a dict with autocomplete of the
    keys that exist in it"""
    added_items: list[ScrapedItem]
    deleted_items: list[ScrapedItem]


class ExcelFormats(TypedDict):
    """A TypedDict for storing various XlsxWriter format objects."""
    merged_header: Format
    """Format for the main section headers (e.g., "Added items")"""
    sub_header: Format
    """Format for the column sub-headers (e.g., "id", "company")"""
    scraped_data: Format
    """Format for the scraped data cells"""


# Define constants
GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH = "/Users/iobreshkov"

JSON_PREV_RUN_OUTPUT_FILE_PREFIX = "previous_run"

XLSX_CURRENT_RUN_OUTPUT_FILE_PREFIX = "previous_run"
XLSX_CHANGES_OUTPUT_FILE_PREFIX = "changes_run"

# This gets computed everytime the `nbim_scraper` module is first loaded by a Python process.
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")

# This gets updated by the `before_exec` function
PREVIOUS_RUN_JSON_FILE = "missing_file"


def _count_number_of_existing_previous_run_files() -> int:
    number_of_prev_run_files = 0
    for filename in os.listdir(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH):
        if filename.endswith(".json") and JSON_PREV_RUN_OUTPUT_FILE_PREFIX in filename:
            number_of_prev_run_files += 1

    return number_of_prev_run_files


def _leave_the_latest_previous_run_file():
    """Leaves only the latest previous run file and deletes the others"""
    previous_run_files = []
    for filename in os.listdir(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH):
        if filename.endswith(".json") and JSON_PREV_RUN_OUTPUT_FILE_PREFIX in filename:
            previous_run_files.append(filename)

    if not previous_run_files:
        return

    # Sort files by date, assuming YYYY-MM-DD format in the filename
    previous_run_files.sort(key=lambda name: name.split('_')[-1].replace('.json', ''), reverse=True)

    # Keep the latest file, delete others
    for file_to_delete in previous_run_files[1:]:
        try:
            os.remove(file_to_delete)
            print(f"Deleted old previous run file: {file_to_delete}")
        except OSError as e:
            print(f"Error deleting file '{file_to_delete}': {e}")


def _write_to_json(scraped_data: list[ScrapedItem], output_file_prefix: str):
    """Helper method that writes the scraped data to a JSON file with the name output_file_prefix_{YYYY-MM-DD}, where
    the timestamp is today's date"""
    filename = f"{output_file_prefix}_{TODAY_DATE}.json"
    full_filepath = os.path.join(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH, filename)

    try:
        with open(full_filepath, 'w') as f:
            json.dump(scraped_data, f, indent=4)
        print(f"Successfully wrote {len(scraped_data)} items to {filename}")
    except IOError as e:
        print(f"Failed to write data to {filename}: {e}")


def _get_json_file_by_prefix(prefix: str) -> str | None:
    """Helper method to get a JSON file by a given prefix. The method assumes no more than one file with the given
    prefix exits.

    Returns:
         the json file name if the file exists or None if the file does not exist
    """

    for filename in os.listdir("."):
        if filename.endswith(".json") and prefix in filename:
            return filename

    return None


def before_exec():
    """Prepares the environment for execution"""
    global PREVIOUS_RUN_JSON_FILE

    # Ensure the storage directory exists
    try:
        os.makedirs(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH}: {e}. Exiting.")
        exit(1) # Exit if we can't create the essential directory

    number_of_prev_run_files = _count_number_of_existing_previous_run_files()
    if number_of_prev_run_files > 1:
        _leave_the_latest_previous_run_file()
        PREVIOUS_RUN_JSON_FILE = _get_json_file_by_prefix(prefix=JSON_PREV_RUN_OUTPUT_FILE_PREFIX) if not None else ""
        return

    if number_of_prev_run_files == 1:
        PREVIOUS_RUN_JSON_FILE = _get_json_file_by_prefix(prefix=JSON_PREV_RUN_OUTPUT_FILE_PREFIX) if not None else ""
        return


def after_exec(scraped_data: list[ScrapedItem]):
    """Does cleanup operations and generation of files"""

    # 1. Generate a previous_run json file
    _write_to_json(scraped_data=scraped_data, output_file_prefix=JSON_PREV_RUN_OUTPUT_FILE_PREFIX)


def scrape_data() -> list[ScrapedItem]:
    raw_data = requests.get(
        "https://www.nbim.no/en/responsible-investment/ethical-exclusions/exclusion-of-companies/").text

    table = Selector(text=raw_data).xpath("//table/tbody/tr")
    if not table:
        print("The location of the table in the HTML might have changed. Please check the page structure again.")
        exit(1)

    scraped_items = []

    for row in table:
        company_raw = row.xpath("string(./td[1])").get()
        decision_raw = row.xpath("./td[5]/text()").get()
        publishing_date_raw = row.xpath("./td[6]/text()").get()

        # Clean up the extracted data
        company = company_raw.strip() if company_raw else None
        decision = decision_raw.strip() if decision_raw else None
        publishing_date = publishing_date_raw.strip() if publishing_date_raw else None

        # Ensure we have the essential data
        if company and decision and publishing_date:
            scraped_item = ScrapedItem(id=_generate_uid_from(company, decision, publishing_date), company=company,
                                       decision=decision, publishing_date=publishing_date)
            scraped_items.append(scraped_item)

        else:
            print(
                f"Skipping row due to missing data.\n"
                f"Company: {company_raw}\n"
                f"Decision: {decision_raw}\n"
                f"Date: {publishing_date}\n"
                f"Row's HTML (first 200 chars): {row.get().strip()}"
            )

    return scraped_items


def _generate_uid_from(company: str, decision: str, publishing_date: str) -> str:
    """Helper method that generates a unique identifier"""

    # In case the caller of the function passes an un-stripped data, we clean it here just to be safe
    company_formatted = company.strip().lower().replace(" ", "-")
    decision_formatted = decision.strip().lower()
    publishing_date_formatted = datetime.strptime(publishing_date.strip(), "%d.%m.%Y").strftime("%Y-%m-%d")

    return f"{company_formatted}-{decision_formatted}-{publishing_date_formatted}"


def deduplicate(scraped_data: list[ScrapedItem]) -> list[ScrapedItem]:
    """In case there are duplicate entries in the NBIM table, we remove those from the scraped data."""
    seen_ids = set()
    unique_items = []
    for item in scraped_data:
        if item["id"] not in seen_ids:
            seen_ids.add(item["id"])
            unique_items.append(item)

    return unique_items


def _check_for_new_items(current_items_map: dict[str, ScrapedItem], previous_items_map: dict[str, ScrapedItem]) -> \
        list[ScrapedItem]:
    """Identifies items present in current data but not in previous data."""
    new_items = []
    for current_item_id, current_item in current_items_map.items():
        if current_item_id not in previous_items_map:
            new_items.append(current_item)

    return new_items


def _check_for_deleted_items(current_items_map: dict[str, ScrapedItem], previous_items_map: dict[str, ScrapedItem]) -> \
        list[ScrapedItem]:
    """Identifies items present in previous data but not in current data."""
    deleted_items = []
    for previous_item_id, previous_item in previous_items_map.items():
        if previous_item_id not in current_items_map:
            deleted_items.append(previous_item)

    return deleted_items


def detect_changes(previous_run_json_filepath: str, current_run_scrapped_data: list[ScrapedItem]) -> ChangesDict | None:
    """Returns the newly added/updated/deleted items to the NBIM list. If no changes are detected, returns None"""
    try:
        with open(previous_run_json_filepath, 'r') as f:
            previous_data: list[ScrapedItem] = json.load(f)
    except FileNotFoundError:
        print(f"Previous run file '{previous_run_json_filepath}' not found. Assuming this is the first run.")
        return None

    except json.JSONDecodeError:
        print(f"Error decoding JSON from '{previous_run_json_filepath}'. No changes returned.")
        return None

    except Exception as e:
        print(f"Unexpected error reading '{previous_run_json_filepath}': {e}. No changes returned.")
        return None

    # Create a dict with key the item id and values the scraped items for easier comparison
    previous_items_map: dict[str, ScrapedItem] = {item['id']: item for item in previous_data}
    current_items_map: dict[str, ScrapedItem] = {item['id']: item for item in current_run_scrapped_data}

    changes = ChangesDict(added_items=[], deleted_items=[])

    new_items = _check_for_new_items(current_items_map, previous_items_map)
    if new_items:
        changes["added_items"] = new_items

    deleted_items = _check_for_deleted_items(current_items_map, previous_items_map)
    if deleted_items:
        changes["deleted_items"] = deleted_items

    return changes if (changes["added_items"] or changes["deleted_items"]) else None


def generate_xlsx_from_scraped_data(scraped_data: list[ScrapedItem], output_file_prefix: str):
    """Generates an Excel file from the whole scraped NBIM table"""

    filename = f"{output_file_prefix}_{TODAY_DATE}.xlsx"
    full_filepath = os.path.join(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH, filename)

    try:
        df = pd.DataFrame(scraped_data)
        sheet_name = "Scraped data"
        df.to_excel(full_filepath, sheet_name=sheet_name, index=False)
        print(f"Successfully wrote {len(scraped_data)} to sheet {sheet_name} in {filename}")
    except Exception as e:
        print(f"Failed to write data to {filename}: {e}")


def _define_excel_formats(workbook: Workbook) -> ExcelFormats:
    """
    Defines and returns a dictionary of XlsxWriter cell formats.

    These formats are used for styling headers and data cells in the Excel sheet.

    Args:
        workbook: The XlsxWriter Workbook object.

    Returns:
        An ExcelFormats TypedDict containing the defined cell formats.
    """
    merged_header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'fg_color': '#DDEBF7'  # Light blue fill
    })
    sub_header_format = workbook.add_format({
        'bold': True,
        'border': 1,
        'fg_color': '#E7E6E6',  # Light grey fill
        'align': 'left'
    })
    scraped_data_format = workbook.add_format({
        'border': 1,
        'align': 'left'
    })

    return ExcelFormats(
        merged_header=merged_header_format,
        sub_header=sub_header_format,
        scraped_data=scraped_data_format
    )


def _write_excel_section(worksheet: Worksheet, items_list: list[ScrapedItem], section_title: str,
                         item_headers: list[str], start_col_index: int, num_item_cols: int, formats: ExcelFormats):
    """
    Writes a complete section (main header, sub-headers, data, and column widths) to the worksheet.

    A section typically represents a category of items, like "Added items" or "Deleted items".

    Args:
        worksheet: The XlsxWriter Worksheet object to write to.
        items_list: A list of ScrapedItem dictionaries for the current section.
        section_title: The title for the section (e.g., "Added items").
        item_headers: A list of strings representing the column headers for the items.
        start_col_index: The starting column index for this section.
        num_item_cols: The number of columns this section will span.
        formats: An ExcelFormats TypedDict containing the cell formats.
    """
    # Main header (e.g., "Added items")
    worksheet.merge_range(0, start_col_index, 0, start_col_index + num_item_cols - 1, section_title,
                          formats['merged_header'])

    # Sub-headers (id, company, etc.)
    for col_num, header_text in enumerate(item_headers):
        worksheet.write(1, start_col_index + col_num, header_text, formats['sub_header'])

    # Data for the current section
    if items_list:
        df_section = pd.DataFrame(items_list, columns=item_headers)
        for r_idx, row_data in enumerate(df_section.values):
            for c_idx, cell_value in enumerate(row_data):
                worksheet.write(r_idx + 2, start_col_index + c_idx, cell_value, formats['scraped_data'])

    # Set column widths for the current section
    if items_list:
        df_for_width = pd.DataFrame(items_list, columns=item_headers)
        for col_idx, header_name in enumerate(item_headers):
            content_max_len = 0
            if not df_for_width[header_name].empty:
                # Calculate max length of content in the column
                # Ensure that .max() on potentially empty or all-NaN series is handled
                series_max_len = df_for_width[header_name].astype(str).map(len).max()
                if pd.notna(series_max_len):
                    content_max_len = series_max_len

            col_width = max(len(header_name), int(content_max_len), 10) + 2  # Min width 10, +2 for padding
            worksheet.set_column(start_col_index + col_idx, start_col_index + col_idx, col_width)
    else:  # No items in this section, set width based on header
        for col_idx, header_name in enumerate(item_headers):
            worksheet.set_column(start_col_index + col_idx, start_col_index + col_idx, len(header_name) + 5)


def generate_xlsx_from_changes(changes: ChangesDict, output_file_prefix: str):
    """
    Generates an Excel file from the changes detected between the current and previous run,
    with "Added items" and "Deleted items" sections side-by-side, separated by a blank column.
    """
    filename = f"{output_file_prefix}_{TODAY_DATE}.xlsx"
    full_filepath = os.path.join(GENERATED_FILES_STORAGE_FOLDER_ABSOLUTE_PATH, filename)


    added_items = changes["added_items"]
    deleted_items = changes["deleted_items"]

    item_headers = list(ScrapedItem.__annotations__.keys())
    num_item_cols = len(item_headers)

    with pd.ExcelWriter(full_filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Changes")

        formats = _define_excel_formats(workbook)

        current_col_offset = 0

        # --- Added Items Section ---
        _write_excel_section(worksheet, added_items, "Added items", item_headers,
                             current_col_offset, num_item_cols, formats)
        current_col_offset += num_item_cols

        # --- Blank Column separator ---
        worksheet.set_column(current_col_offset, current_col_offset, 3)  # Width for the blank column
        current_col_offset += 1

        # --- Deleted Items Section ---
        _write_excel_section(worksheet, deleted_items, "Deleted items", item_headers,
                             current_col_offset, num_item_cols, formats)

    # Logging
    log_parts = [f"{len(added_items)} added items", f"{len(deleted_items)} deleted items"]
    print(f"Successfully wrote {', '.join(log_parts)} to sheet Changes in {filename}")


def send_notification_email():
    # TODO: Implement
    pass


def scrape_flow():
    before_exec()

    # 1. Get the data and remove duplicates if any
    scraped_data = deduplicate(scraped_data=scrape_data())

    # 3. Write the whole scrapped data to an Excel file
    generate_xlsx_from_scraped_data(scraped_data=scraped_data, output_file_prefix=XLSX_CURRENT_RUN_OUTPUT_FILE_PREFIX)

    # 4. Check for changes comparing to a previous run of the script
    changes = detect_changes(previous_run_json_filepath=PREVIOUS_RUN_JSON_FILE,
                             current_run_scrapped_data=scraped_data)
    if changes is None:
        print("No changes detected")
        after_exec(scraped_data=scraped_data)
        return

    # 5. Generate an Excel file from the changes
    generate_xlsx_from_changes(changes=changes, output_file_prefix=XLSX_CHANGES_OUTPUT_FILE_PREFIX)

    # 6. Send an email with the Excel file as attachment
    send_notification_email()

    after_exec(scraped_data=scraped_data)


if __name__ == "__main__":
    scrape_flow()
