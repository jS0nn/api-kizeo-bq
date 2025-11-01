// SubformUtils Version 0.1.0

var SUBFORM_FIELD_TYPES = ['subform', 'table', 'tableau', 'table_subform', 'table subform'];

function normalizeSubformRows(rawValue) {
  if (!rawValue) return [];

  if (Array.isArray(rawValue)) {
    return rawValue
      .map(function (row) {
        return normalizeSubformRow(row);
      })
      .filter(function (row) {
        return row && Object.keys(row).length;
      });
  }

  if (typeof rawValue === 'object') {
    if (Array.isArray(rawValue.rows)) {
      return rawValue.rows
        .map(function (row) {
          return normalizeSubformRow(row);
        })
        .filter(function (row) {
          return row && Object.keys(row).length;
        });
    }
    if (Array.isArray(rawValue.data)) {
      return rawValue.data
        .map(function (row) {
          return normalizeSubformRow(row);
        })
        .filter(function (row) {
          return row && Object.keys(row).length;
        });
    }
    if (isLikelySubformRow(rawValue)) {
      var normalizedRow = normalizeSubformRow(rawValue);
      return Object.keys(normalizedRow).length ? [normalizedRow] : [];
    }
    return normalizeSubformRows(Object.values(rawValue));
  }

  if (typeof rawValue === 'string') {
    var trimmed = rawValue.trim();
    if (!trimmed) return [];
    try {
      var parsed = JSON.parse(trimmed);
      return normalizeSubformRows(parsed);
    } catch (e) {
      return [];
    }
  }

  return [];
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
