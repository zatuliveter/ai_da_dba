import xml.etree.ElementTree as ET

import yaml

from backend.db import get_connection


def _parse_execution_plan(xml_plan: str) -> str:
    """Parse SHOWPLAN_XML into a readable summary."""
    ns = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
    try:
        root = ET.fromstring(xml_plan)
    except ET.ParseError:
        return yaml.dump({"raw_plan": xml_plan[:4000]}, allow_unicode=True)

    statements = []
    for stmt in root.findall(".//sp:StmtSimple", ns):
        stmt_text = stmt.get("StatementText", "")
        est_rows = stmt.get("StatementEstRows", "")
        est_cost = stmt.get("StatementSubTreeCost", "")

        operators = []
        for rel_op in stmt.findall(".//sp:RelOp", ns):
            op_info = {
                "operation": rel_op.get("PhysicalOp", ""),
                "logical_op": rel_op.get("LogicalOp", ""),
                "est_rows": rel_op.get("EstimateRows", ""),
                "est_cost": rel_op.get("EstimatedTotalSubtreeCost", ""),
                "est_cpu": rel_op.get("EstimateCPU", ""),
                "est_io": rel_op.get("EstimateIO", ""),
            }
            for obj in rel_op.findall(".//sp:Object", ns):
                op_info["table"] = obj.get("Table", "").strip("[]")
                op_info["index"] = obj.get("Index", "").strip("[]")
                op_info["schema"] = obj.get("Schema", "").strip("[]")

            for warn in rel_op.findall(".//sp:Warnings", ns):
                warnings = []
                for child in warn:
                    tag = child.tag.replace(f"{{{ns['sp']}}}", "")
                    warnings.append(tag)
                if warnings:
                    op_info["warnings"] = warnings

            operators.append(op_info)

        statements.append({
            "statement": stmt_text.strip()[:200],
            "estimated_rows": est_rows,
            "estimated_cost": est_cost,
            "operators": operators,
        })

    missing_indexes = []
    for mg in root.findall(".//sp:MissingIndexGroup", ns):
        impact = mg.get("Impact", "")
        for mi in mg.findall(".//sp:MissingIndex", ns):
            table = mi.get("Table", "").strip("[]")
            schema = mi.get("Schema", "").strip("[]")
            eq_cols = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='EQUALITY']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            inequality_columns = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='INEQUALITY']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            incl_cols = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='INCLUDE']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            missing_indexes.append({
                "table": f"{schema}.{table}",
                "impact": impact,
                "equality_columns": eq_cols or None,
                "inequality_columns": inequality_columns or None,
                "include_columns": incl_cols or None,
            })

    result = {"statements": statements}
    if missing_indexes:
        result["missing_indexes"] = missing_indexes

    return yaml.dump(result, allow_unicode=True)


def get_execution_plan(database: str, query: str) -> str:
    """Get estimated execution plan and return a text summary."""
    with get_connection(database) as conn:
        cursor = conn.cursor()
        cursor.execute("SET SHOWPLAN_XML ON")
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.execute("SET SHOWPLAN_XML OFF")

    if not row:
        return yaml.dump({"error": "No execution plan returned"}, allow_unicode=True)

    xml_plan = row[0]
    return _parse_execution_plan(xml_plan)


definition = {
    "type": "function",
    "function": {
        "name": "get_execution_plan",
        "description": "Get the estimated execution plan for a SQL query. Returns operators, costs, row estimates, and missing index hints. Use this to analyze query performance.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The SQL query to analyze"},
            },
            "required": ["query"],
        },
    },
}
