import requests
import frappe
from datetime import datetime, timedelta
from frappe.utils import get_datetime, now_datetime
import traceback


def checkin_exists(employee, punch_dt):
    # Treat any punch within the same minute as duplicate
    start = punch_dt.replace(second=0, microsecond=0)
    end = start + timedelta(minutes=1)

    return frappe.db.exists(
        "Employee Checkin",
        {
            "employee": employee,
            "device_id": "BioTime",
            "time": ["between", [start, end]],
        },
    )


@frappe.whitelist()
def biotime_attendance():
    frappe.enqueue(
        "biotime_erpgulf.attendance.run_biotime_attendance",
        queue="long",
        job_name="BioTime Datetime Sync",
    )
    return {"message": "BioTime sync started"}


def run_biotime_attendance():
    logger = frappe.logger("biotime")

    try:
        settings = frappe.get_single("BioTime Settings")
    except Exception:
        frappe.throw("BioTime Settings DocType not found")

    if not settings.start_year:
        frappe.throw("Start Year is mandatory in BioTime Settings")

    now_dt = now_datetime()

    if settings.last_synced_datetime:
        start_dt = get_datetime(settings.last_synced_datetime)
        if start_dt > now_dt:
            start_dt = now_dt
    else:
        start_dt = datetime(int(settings.start_year), 1, 1)

    end_dt = start_dt + timedelta(days=30)
    if end_dt > now_dt:
        end_dt = now_dt

    logger.info(f"BioTime sync window: {start_dt} → {end_dt}")

    if start_dt >= end_dt:
        logger.info("Nothing to sync. Start datetime >= end datetime.")
        return "No new data to sync"

    base_url = settings.biotime_url.rstrip("/") + "/iclock/api/transactions/"
    headers = {"Authorization": f"Token {settings.biotime_token}"}

    inserted = 0
    skipped = 0
    page = 1

    while True:
        try:
            response = requests.get(
                base_url,
                headers=headers,
                params={
                    "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "page": page,
                },
                timeout=90,
            )
            response.raise_for_status()
            payload = response.json()
            logger.info(payload)
            rows = payload.get("data") or []

        except Exception:
            logger.exception("BioTime API failed")
            break

        if not rows:
            break

        for row in rows:
            try:
                logger.info(f"BioTime Row: {row}")
                # emp_code = row.get("emp_code")
                emp_code = str(row.get("emp_code")).strip()
                punch_time = row.get("punch_time")
                # punch_state = row.get("punch_state_display")
                punch_state = row.get("punch_state_display") or row.get("punch_state")
                area_alias = row.get("area_alias") or None

                if not (emp_code and punch_time and punch_state):
                    skipped += 1
                    continue

                punch_dt = get_datetime(punch_time)

                employee = frappe.db.get_value(
                    "Employee",
                    {"biotime_emp_code": emp_code},
                    "name",
                )

                if not employee:
                    skipped += 1
                    continue

                # Prevent duplicates (same minute)
                # if checkin_exists(employee, punch_dt):
                #     skipped += 1
                #     continue
                
                biotime_id = row.get("id")

                if not biotime_id:
                    skipped += 1
                    continue

                # DUPLICATE CHECK USING UNIQUE ID
                if frappe.db.exists("Employee Checkin", {"custom_biotime_id": biotime_id}):
                    skipped += 1
                    continue


                # log_type = "IN" if punch_state == "Check In" else "OUT"
                log_type = "IN" if punch_state in ["Check In", "0"] else "OUT"

                try:
                    frappe.get_doc(
                        {
                            "doctype": "Employee Checkin",
                            "employee": employee,
                            "time": punch_dt,
                            "log_type": log_type,
                            "device_id": "BioTime",
                            "custom_location_id": area_alias,
                            "custom_biotime_id": biotime_id
                        }
                    ).insert(ignore_permissions=True)

                    inserted += 1

                except frappe.UniqueValidationError:
                    # DB-level duplicate protection
                    skipped += 1

            except Exception:
                logger.exception("Row insert failed")
                skipped += 1

        if payload.get("next"):
            page += 1
        else:
            break

    frappe.db.set_value(
        "BioTime Settings",
        None,
        "last_synced_datetime",
        end_dt,
    )
    frappe.db.commit()

    logger.info(f"BioTime sync done. Inserted={inserted}, Skipped={skipped}")
    return f"Inserted={inserted}, Skipped={skipped}"
