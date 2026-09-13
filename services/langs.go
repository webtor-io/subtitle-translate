package services

// langNames maps the 2-letter codes the service accepts to the English
// language name used in the prompt. Kept broader than the 11 UI locales:
// the preferred language comes from the profile setting.
var langNames = map[string]string{
	"en": "English", "ru": "Russian", "es": "Spanish", "de": "German", "fr": "French",
	"pt": "Portuguese", "it": "Italian", "pl": "Polish", "tr": "Turkish", "nl": "Dutch",
	"cs": "Czech", "uk": "Ukrainian", "zh": "Chinese", "ja": "Japanese", "ko": "Korean",
	"ar": "Arabic", "hi": "Hindi", "id": "Indonesian", "vi": "Vietnamese", "th": "Thai",
	"sv": "Swedish", "no": "Norwegian", "da": "Danish", "fi": "Finnish", "el": "Greek",
	"he": "Hebrew", "hu": "Hungarian", "ro": "Romanian", "bg": "Bulgarian", "sr": "Serbian",
	"hr": "Croatian", "sk": "Slovak", "sl": "Slovenian", "lt": "Lithuanian", "lv": "Latvian",
	"et": "Estonian", "fa": "Persian", "ms": "Malay", "bn": "Bengali", "ta": "Tamil",
	"kk": "Kazakh", "ka": "Georgian", "hy": "Armenian", "az": "Azerbaijani", "ca": "Catalan",
}

func LangName(code string) (string, bool) {
	n, ok := langNames[code]
	return n, ok
}
