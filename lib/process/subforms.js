// SubformUtils Version 0.2.0

var SUBFORM_FIELD_TYPES = ['subform', 'table', 'tableau', 'table_subform', 'table subform'];

function normalizeSubformRows(rawValue) {
  var rawRows = resolveRawSubformRows(rawValue);
  if (!rawRows.length) return [];

  return rawRows
    .map(function (row) {
      return normalizeSubformRow(row);
    })
    .filter(function (row) {
      return row && Object.keys(row).length;
    });
}

function normalizeSubformRow(row) {
  if (!row || typeof row !== 'object') {
    return {};
  }

  var source = row.fields && typeof row.fields === 'object' ? row.fields : row;
  var normalized = {};

  Object.keys(source).forEach(function (key) {
    var cell = source[key];
    if (cell && typeof cell === 'object' && cell.hasOwnProperty('value')) {
      normalized[key] = cell.value;
    } else {
      normalized[key] = cell;
    }
  });

  return normalized;
}

function extractSubformRowSource(row) {
  if (!row || typeof row !== 'object') {
    return null;
  }
  if (row.fields && typeof row.fields === 'object') {
    return row.fields;
  }
  return row;
}

function isLikelySubformRow(obj) {
  if (!obj || typeof obj !== 'object') return false;
  if (obj.fields && typeof obj.fields === 'object') return true;
  return Object.keys(obj).some(function (key) {
    var cell = obj[key];
    return cell && typeof cell === 'object' && cell.hasOwnProperty('value');
  });
}

function isSubformField(fieldType, fieldValue) {
  var normalizedType = (fieldType || '').toString().toLowerCase();
  if (
    SUBFORM_FIELD_TYPES.some(function (type) {
      return normalizedType === type || normalizedType.indexOf(type) !== -1;
    })
  ) {
    return true;
  }

  var rows = normalizeSubformRows(fieldValue);
  return rows.length > 0;
}

function resolveRawSubformRows(rawValue) {
  if (!rawValue) return [];

  if (Array.isArray(rawValue)) {
    return rawValue;
  }

  if (typeof rawValue === 'object') {
    if (Array.isArray(rawValue.rows)) {
      return rawValue.rows;
    }
    if (Array.isArray(rawValue.data)) {
      return rawValue.data;
    }
    if (isLikelySubformRow(rawValue)) {
      return [rawValue];
    }
    var nestedRows = [];
    Object.keys(rawValue).forEach(function (key) {
      var childRows = resolveRawSubformRows(rawValue[key]);
      if (childRows.length) {
        nestedRows = nestedRows.concat(childRows);
      }
    });
    return nestedRows;
  }

  if (typeof rawValue === 'string') {
    var trimmed = rawValue.trim();
    if (!trimmed) return [];
    try {
      var parsed = JSON.parse(trimmed);
      return resolveRawSubformRows(parsed);
    } catch (e) {
      return [];
    }
  }

  return [];
}

function getSubformRowSources(rawValue) {
  var rawRows = resolveRawSubformRows(rawValue);
  if (!rawRows.length) return [];

  return rawRows
    .map(function (row, index) {
      var source = extractSubformRowSource(row);
      if (!source || typeof source !== 'object') {
        return null;
      }
      return { index: index, source: source };
    })
    .filter(function (entry) {
      return entry && Object.keys(entry.source).length;
    });
}
