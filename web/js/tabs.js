function init_tabs(root_id){
	var root = document.getElementById(root_id)
	if(root) root.querySelector('button.toggle_tabs').onclick = function(){change_tab(root)};
}

function change_tab(root){
	var tabs = root.querySelectorAll('.tab');
	for(var i = 0; i < tabs.length; i++){
		if(tabs[i].classList.contains('selected')){
			break;
		}
	}
	tabs[i].classList.remove('selected');
	var j = i+1 < tabs.length ? i+1 : 0;
	tabs[j].classList.add('selected');
	var k = j+1 < tabs.length ? j+1 : 0;
	var tabname = tabs[k].getAttribute('data-tab-name');
	if(tabname){
		var button = root.querySelector('button.toggle_tabs')
		var prefix = button.getAttribute('data-prefix');
		button.textContent = prefix+tabname;
	}
}