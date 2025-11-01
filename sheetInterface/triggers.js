// triggers Version 0.1.0

var sheetTriggers =
  typeof sheetTriggers !== 'undefined'
    ? sheetTriggers
    : (function () {
        var TRIGGER_DISABLED_KEY = 'none';
        var DEFAULT_TRIGGER_FREQUENCY = 'H24';
        var TRIGGER_FREQUENCY_PROPERTY = 'TRIGGER_FREQUENCY';
        var TRIGGER_OPTIONS = {
          M1: { type: 'M', value: 1, label: 'Toutes les minutes' },
          M10: { type: 'M', value: 10, label: 'Toutes les 10 minutes' },
          M30: { type: 'M', value: 30, label: 'Toutes les 30 minutes' },
          H1: { type: 'H', value: 1, label: 'Toutes les heures' },
          H3: { type: 'H', value: 3, label: 'Toutes les 3 heures' },
          H6: { type: 'H', value: 6, label: 'Toutes les 6 heures' },
          H24: { type: 'H', value: 24, label: 'Une fois par jour' },
          WD1: { type: 'D', value: 7, label: 'Une fois par semaine' }
        };
        var DAILY_CUSTOM_TRIGGER_PATTERN = /^H24@([01]\d|2[0-3])$/;
        var WEEKLY_CUSTOM_TRIGGER_PATTERN = /^WD1@([A-Z]{3})@([01]\d|2[0-3])$/;
        var WEEKDAY_CODE_MAP = {
          MON: { scriptEnum: ScriptApp.WeekDay.MONDAY, label: 'lundi' },
          TUE: { scriptEnum: ScriptApp.WeekDay.TUESDAY, label: 'mardi' },
          WED: { scriptEnum: ScriptApp.WeekDay.WEDNESDAY, label: 'mercredi' },
          THU: { scriptEnum: ScriptApp.WeekDay.THURSDAY, label: 'jeudi' },
          FRI: { scriptEnum: ScriptApp.WeekDay.FRIDAY, label: 'vendredi' },
          SAT: { scriptEnum: ScriptApp.WeekDay.SATURDAY, label: 'samedi' },
          SUN: { scriptEnum: ScriptApp.WeekDay.SUNDAY, label: 'dimanche' }
        };

        function sanitizeTriggerFrequency(raw) {
          if (raw === null || raw === undefined) return DEFAULT_TRIGGER_FREQUENCY;
          var stringValue = raw.toString().trim();
          if (!stringValue) return DEFAULT_TRIGGER_FREQUENCY;
          var lower = stringValue.toLowerCase();
          if (lower === TRIGGER_DISABLED_KEY) return TRIGGER_DISABLED_KEY;
          var upper = stringValue.toUpperCase();
          if (TRIGGER_OPTIONS[upper]) return upper;
          if (DAILY_CUSTOM_TRIGGER_PATTERN.test(upper)) return upper;
          if (WEEKLY_CUSTOM_TRIGGER_PATTERN.test(upper)) return upper;
          return DEFAULT_TRIGGER_FREQUENCY;
        }

        function parseCustomDailyHour(key) {
          if (!key) return null;
          var match = DAILY_CUSTOM_TRIGGER_PATTERN.exec(key.toUpperCase());
          if (!match) return null;
          var hour = Number(match[1]);
          if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
            return null;
          }
          return hour;
        }

        function formatHourLabel(hour) {
          var normalized = Math.min(23, Math.max(0, Math.floor(hour)));
          return normalized.toString().padStart(2, '0') + 'h00';
        }

        function parseCustomWeekly(key) {
          if (!key) return null;
          var match = WEEKLY_CUSTOM_TRIGGER_PATTERN.exec(key.toUpperCase());
          if (!match) return null;
          var dayCode = match[1];
          var hour = Number(match[2]);
          if (!WEEKDAY_CODE_MAP[dayCode]) {
            return null;
          }
          if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
            return null;
          }
          return { dayCode: dayCode, hour: hour };
        }

        function formatWeekdayLabel(dayCode) {
          var entry = WEEKDAY_CODE_MAP[dayCode];
          if (entry && entry.label) {
            return entry.label;
          }
          return dayCode || 'jour';
        }

        function getTriggerOption(key) {
          if (!key) return null;
          if (TRIGGER_OPTIONS[key]) {
            return TRIGGER_OPTIONS[key];
          }
          var customHour = parseCustomDailyHour(key);
          if (customHour !== null) {
            return {
              type: 'CUSTOM_DAILY',
              value: 24,
              label: 'Chaque jour à ' + formatHourLabel(customHour),
              hour: customHour
            };
          }
          var customWeekly = parseCustomWeekly(key);
          if (customWeekly) {
            return {
              type: 'CUSTOM_WEEKLY',
              value: 7,
              label:
                'Chaque semaine le ' +
                formatWeekdayLabel(customWeekly.dayCode) +
                ' à ' +
                formatHourLabel(customWeekly.hour),
              dayCode: customWeekly.dayCode,
              hour: customWeekly.hour
            };
          }
          return null;
        }

        function describeTriggerOption(key) {
          if (!key) return 'inconnue';
          if (key === TRIGGER_DISABLED_KEY) return 'désactivée';
          var option = getTriggerOption(key);
          if (!option) return key;
          if (option.type === 'CUSTOM_DAILY' && typeof option.hour === 'number') {
            return option.label;
          }
          if (option.type === 'CUSTOM_WEEKLY' && typeof option.hour === 'number') {
            return option.label;
          }
          var unit = option.type === 'M' ? 'minute' : 'heure';
          var plural = option.value > 1 ? 's' : '';
          return option.value + ' ' + unit + plural;
        }

        function configureTriggerFromKey(key) {
          if (key === TRIGGER_DISABLED_KEY) {
            var mainHandler = typeof MAIN_TRIGGER_FUNCTION === 'undefined' ? 'main' : MAIN_TRIGGER_FUNCTION;
            var dedupHandler =
              typeof DEDUP_TRIGGER_FUNCTION === 'undefined' ? 'runBigQueryDeduplication' : DEDUP_TRIGGER_FUNCTION;
            deleteTriggersByFunction(mainHandler);
            deleteTriggersByFunction(dedupHandler);
            console.log('Déclencheurs automatiques désactivés.');
            return null;
          }
          var option = getTriggerOption(key);
          if (!option) {
            throw new Error('Fréquence de déclencheur inconnue: ' + key);
          }
          if (option.type === 'CUSTOM_DAILY') {
            configurerDeclencheurQuotidienAvecHeure(option.hour);
          } else if (option.type === 'CUSTOM_WEEKLY') {
            configurerDeclencheurHebdomadaire(option.dayCode, option.hour);
          } else {
            configurerDeclencheurHoraire(option.value, option.type);
          }
          ensureDeduplicationTrigger();
          return option;
        }

        function getStoredTriggerFrequency() {
          var props = PropertiesService.getScriptProperties();
          var rawValue = props.getProperty(TRIGGER_FREQUENCY_PROPERTY);
          var sanitized = sanitizeTriggerFrequency(rawValue);
          if (!rawValue || rawValue !== sanitized) {
            props.setProperty(TRIGGER_FREQUENCY_PROPERTY, sanitized);
          }
          return sanitized;
        }

        function setStoredTriggerFrequency(key) {
          var sanitized = sanitizeTriggerFrequency(key);
          PropertiesService.getScriptProperties().setProperty(TRIGGER_FREQUENCY_PROPERTY, sanitized);
          return sanitized;
        }

        function persistTriggerFrequencyToSheet(frequencyKey) {
          try {
            var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
            if (!spreadsheet) return;
            var activeSheet = spreadsheet.getActiveSheet();
            if (!activeSheet) return;
            if (typeof sheetConfig === 'undefined') return;
            var existingConfig = sheetConfig.readFormConfigFromSheet(activeSheet) || {};
            var sanitized = sanitizeTriggerFrequency(frequencyKey);
            if (
              existingConfig.trigger_frequency &&
              existingConfig.trigger_frequency.toString().trim() === sanitized
            ) {
              return;
            }
            var mergedConfig = Object.assign({}, existingConfig, {
              trigger_frequency: sanitized
            });
            sheetConfig.writeFormConfigToSheet(activeSheet, mergedConfig);
          } catch (e) {
            uiHandleException('persistTriggerFrequencyToSheet', e, { frequencyKey: frequencyKey });
          }
        }

        function getTriggerOptions() {
          return TRIGGER_OPTIONS;
        }

        return {
          TRIGGER_DISABLED_KEY: TRIGGER_DISABLED_KEY,
          DEFAULT_TRIGGER_FREQUENCY: DEFAULT_TRIGGER_FREQUENCY,
          TRIGGER_FREQUENCY_PROPERTY: TRIGGER_FREQUENCY_PROPERTY,
          TRIGGER_OPTIONS: TRIGGER_OPTIONS,
          sanitizeTriggerFrequency: sanitizeTriggerFrequency,
          getTriggerOption: getTriggerOption,
          describeTriggerOption: describeTriggerOption,
          configureTriggerFromKey: configureTriggerFromKey,
          parseCustomDailyHour: parseCustomDailyHour,
          formatHourLabel: formatHourLabel,
          parseCustomWeekly: parseCustomWeekly,
          formatWeekdayLabel: formatWeekdayLabel,
          getStoredTriggerFrequency: getStoredTriggerFrequency,
          setStoredTriggerFrequency: setStoredTriggerFrequency,
          persistTriggerFrequencyToSheet: persistTriggerFrequencyToSheet,
          getTriggerOptions: getTriggerOptions,
          DAILY_CUSTOM_TRIGGER_PATTERN: DAILY_CUSTOM_TRIGGER_PATTERN,
          WEEKLY_CUSTOM_TRIGGER_PATTERN: WEEKLY_CUSTOM_TRIGGER_PATTERN,
          WEEKDAY_CODE_MAP: WEEKDAY_CODE_MAP
        };
      })();

var TRIGGER_DISABLED_KEY = sheetTriggers.TRIGGER_DISABLED_KEY;
var TRIGGER_FREQUENCY_PROPERTY = sheetTriggers.TRIGGER_FREQUENCY_PROPERTY;
var TRIGGER_OPTIONS = sheetTriggers.TRIGGER_OPTIONS;
var DAILY_CUSTOM_TRIGGER_PATTERN = sheetTriggers.DAILY_CUSTOM_TRIGGER_PATTERN;
var WEEKLY_CUSTOM_TRIGGER_PATTERN = sheetTriggers.WEEKLY_CUSTOM_TRIGGER_PATTERN;
var WEEKDAY_CODE_MAP = sheetTriggers.WEEKDAY_CODE_MAP;
