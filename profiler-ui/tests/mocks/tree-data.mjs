// language=javascript
export const mockTreeData = `treedata(0, function(){app.args={}; app.args['params-trim-size']=10000;
app.durationFormat='TIME';
CT.updateFormatFromPersonalSettings();
var S=CT.sqls, B=CT.xmls;
var t;
t=CT.tags;t.a( 0, "void org.example.TestClass.testMethod()");t.a( 1, "void org.example.Jdbc.executeBatch()");t.a( 2, "sql");t.b( "sql",0, 1000,0, "");
var s={}; var x={}; var tc;
tc=s[ "100/1/0"]= "select 1";
tc=s[ "200/1/0"]= "select 2";
tc=s[ "300/1/0"]= "select 3";
t =  [0,40,10,0,0,1,0,9223372036854775807,-9223372036854775808,[[1,30,30,0,0,1,0,0,1,[],0,[[2,20,1,[s["100/1/0"],s["200/1/0"]]],[2,10,1,[s["300/1/0"]]]]]
],0];
t = CT.append(t, []);
CT.timeRange( 9223372036854775807, -9223372036854775808);CT.setCategories(t,CT.defaultCategories);CT.setAdjustments(t,"");return t;})`;
