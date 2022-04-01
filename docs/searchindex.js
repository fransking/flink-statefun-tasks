Search.setIndex({docnames:["actions","api","api/flink_tasks","api/flink_tasks_client","api/generated/statefun_tasks.DefaultSerialiser","api/generated/statefun_tasks.FlinkTasks","api/generated/statefun_tasks.PipelineBuilder","api/generated/statefun_tasks.TaskContext","api/generated/statefun_tasks.client.FlinkTasksClient","api/generated/statefun_tasks.client.FlinkTasksClientFactory","api/generated/statefun_tasks.client.TaskStatus","api/generated/statefun_tasks.events.EventHandlers","deployment_topologies","events","extensions","getting_started","index","intro","pipelines","stateful_tasks","tasks","tasks_vs_functions"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":3,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":2,"sphinx.domains.rst":2,"sphinx.domains.std":2,sphinx:56},filenames:["actions.rst","api.rst","api/flink_tasks.rst","api/flink_tasks_client.rst","api/generated/statefun_tasks.DefaultSerialiser.rst","api/generated/statefun_tasks.FlinkTasks.rst","api/generated/statefun_tasks.PipelineBuilder.rst","api/generated/statefun_tasks.TaskContext.rst","api/generated/statefun_tasks.client.FlinkTasksClient.rst","api/generated/statefun_tasks.client.FlinkTasksClientFactory.rst","api/generated/statefun_tasks.client.TaskStatus.rst","api/generated/statefun_tasks.events.EventHandlers.rst","deployment_topologies.rst","events.rst","extensions.rst","getting_started.rst","index.rst","intro.rst","pipelines.rst","stateful_tasks.rst","tasks.rst","tasks_vs_functions.rst"],objects:{"statefun_tasks.DefaultSerialiser":{__init__:[4,1,1,""],deserialise_args_and_kwargs:[4,1,1,""],deserialise_request:[4,1,1,""],deserialise_result:[4,1,1,""],from_proto:[4,1,1,""],merge_args_and_kwargs:[4,1,1,""],serialise_args_and_kwargs:[4,1,1,""],serialise_request:[4,1,1,""],serialise_result:[4,1,1,""],to_args_and_kwargs:[4,1,1,""],to_proto:[4,1,1,""],unpack_response:[4,1,1,""]},"statefun_tasks.FlinkTasks":{__init__:[5,1,1,""],bind:[5,1,1,""],clone_task_request:[5,1,1,""],events:[5,1,1,""],extend:[5,1,1,""],fail:[5,1,1,""],get_task:[5,1,1,""],register:[5,1,1,""],register_builtin:[5,1,1,""],run_async:[5,1,1,""],send:[5,1,1,""],send_result:[5,1,1,""],set_storage_backend:[5,1,1,""],unpack_task_request:[5,1,1,""]},"statefun_tasks.PipelineBuilder":{__init__:[6,1,1,""],append_group:[6,1,1,""],append_to:[6,1,1,""],continue_if:[6,1,1,""],continue_with:[6,1,1,""],finally_do:[6,1,1,""],from_proto:[6,1,1,""],get_destination:[6,1,1,""],get_tasks:[6,1,1,""],id:[6,1,1,""],is_empty:[6,1,1,""],send:[6,1,1,""],set:[6,1,1,""],to_pipeline:[6,1,1,""],to_proto:[6,1,1,""],to_task_request:[6,1,1,""],validate:[6,1,1,""],wait:[6,1,1,""]},"statefun_tasks.TaskContext":{__init__:[7,1,1,""],contextualise_from:[7,1,1,""],get_address:[7,1,1,""],get_caller_address:[7,1,1,""],get_caller_id:[7,1,1,""],get_parent_task_address:[7,1,1,""],get_parent_task_id:[7,1,1,""],get_pipeline_address:[7,1,1,""],get_pipeline_id:[7,1,1,""],get_root_pipeline_address:[7,1,1,""],get_root_pipeline_id:[7,1,1,""],get_task_id:[7,1,1,""],send_egress_message:[7,1,1,""],send_message:[7,1,1,""],send_message_after:[7,1,1,""],task_name:[7,1,1,""],task_uid:[7,1,1,""],to_address_and_id:[7,1,1,""]},"statefun_tasks.client":{FlinkTasksClient:[8,0,1,""],FlinkTasksClientFactory:[9,0,1,""],TaskStatus:[10,0,1,""]},"statefun_tasks.client.FlinkTasksClient":{__init__:[8,1,1,""],cancel_pipeline:[8,1,1,""],cancel_pipeline_async:[8,1,1,""],get_request:[8,1,1,""],get_request_async:[8,1,1,""],get_result:[8,1,1,""],get_result_async:[8,1,1,""],get_status:[8,1,1,""],get_status_async:[8,1,1,""],pause_pipeline:[8,1,1,""],pause_pipeline_async:[8,1,1,""],serialiser:[8,1,1,""],submit:[8,1,1,""],submit_async:[8,1,1,""],unpause_pipeline:[8,1,1,""],unpause_pipeline_async:[8,1,1,""]},"statefun_tasks.client.FlinkTasksClientFactory":{__init__:[9,1,1,""],get_client:[9,1,1,""]},"statefun_tasks.client.TaskStatus":{__init__:[10,1,1,""]},"statefun_tasks.events":{EventHandlers:[11,0,1,""]},"statefun_tasks.events.EventHandlers":{__init__:[11,1,1,""],notify_pipeline_created:[11,1,1,""],notify_pipeline_finished:[11,1,1,""],notify_pipeline_status_changed:[11,1,1,""],notify_pipeline_task_finished:[11,1,1,""],notify_task_finished:[11,1,1,""],notify_task_retry:[11,1,1,""],notify_task_started:[11,1,1,""],on_pipeline_created:[11,1,1,""],on_pipeline_finished:[11,1,1,""],on_pipeline_status_changed:[11,1,1,""],on_pipeline_task_finished:[11,1,1,""],on_task_finished:[11,1,1,""],on_task_retry:[11,1,1,""],on_task_started:[11,1,1,""]},statefun_tasks:{DefaultSerialiser:[4,0,1,""],FlinkTasks:[5,0,1,""],PipelineBuilder:[6,0,1,""],TaskContext:[7,0,1,""]}},objnames:{"0":["py","class","Python class"],"1":["py","method","Python method"]},objtypes:{"0":"py:class","1":"py:method"},terms:{"100":18,"case":[5,13,14],"class":[4,5,6,7,8,9,10,11,14],"default":[4,5,6,8,9,14,19],"final":[6,18],"function":[5,6,12,14,16,18,19,20],"import":14,"new":6,"return":[4,5,6,7,8,9,12,13,14,17,18,19,20,21],"static":[4,5,6,7,9],"throw":[5,18],"true":[5,6,13,17,18,19,20,21],"try":17,For:[13,17],K8s:13,Not:7,The:[5,6,7,12,13,14,17,18,21],There:14,These:[13,14,17],__builtin:[5,6],__init__:[4,5,6,7,8,9,10,11],_load_pric:17,a_long_running_task:0,abil:21,accept:[14,17],accur:[4,5,6,7,8,9,10,11],achiev:17,across:18,act:12,action:[8,9,16],action_top:[8,9],action_topt:[8,9],add:[5,6,18],add_posit:18,add_stat:18,added:[6,19],addit:[5,7,8,9],address:[5,7,19,21],after:7,aggreg:[14,18],aio_work:12,all:[6,11,12,17],allow:17,alreadi:4,also:[5,17,18,19,21],alwai:17,ani:[4,5,6,7,18,20],anoth:[6,7,12,18,21],apach:[16,17],api:[5,16,17],app:17,append:[6,17],append_group:6,append_to:6,appli:14,approach:17,arg:[4,5,6,18],argsandkwarg:4,argument:[6,18],around:[7,17,18],arrai:[4,17],as_typ:[17,21],assertequ:21,associ:[5,12],async:[5,8,12,17,20],attribut:[5,6,7,8,10,20,21],automat:6,averag:[17,18],avg:17,await:[0,12,14,17,18,19,20],back:[4,17,20],backend:[5,14],band:0,base:[14,17],becom:[12,17],befor:[0,14,17],begin:13,behaviour:[7,14],being:14,best:14,better:17,beyond:14,bind:[5,6,12,14,17,18,19,20,21],block:0,bool:[5,6],borrow:17,bound:12,broker:[8,9],bucket:5,builder:[5,6],built:5,calcul:[17,19],call:[7,11,12,17,18,21],caller:[5,7,13,14,17,18,20,21],can:[5,13,14,15,17,18,19,20],cancel:8,cancel_pipelin:8,cancel_pipeline_async:[0,8],cannot:17,caught:18,caus:[5,6,14],celeri:17,central:17,chain:17,check:16,check_result:18,classif:12,clean:18,cleanup:18,clear:21,clearli:14,client:[0,1,8,9,10,12,14,18,19,20],clone_task_request:5,code:[14,16],combin:[17,18],complet:[0,5,13],compos:[16,17,19],comput:13,compute_averag:17,compute_average_std_devs_of_timeseri:17,compute_std_dev:17,concept:17,concern:12,concretis:6,concurr:18,condit:6,condition:6,configur:14,confus:7,connect:12,consider:14,constrain:12,constraint:14,construct:[18,19],consum:[8,9],contain:[5,6],context:[5,7,11,13,17,19,20,21],contextualise_from:7,continu:[6,17],continue_if:6,continue_with:[0,6,12,17,18],contiun:18,conveni:14,convert:[4,7],copi:5,correspond:[12,19,21],could:[14,17],count_to_100:18,cpu:12,cpu_work:12,creat:[6,8,9],customstoragebackend:14,daign:17,data:[14,17],debugg:17,declar:[14,18],decor:[5,6,11,14,17,19],dedic:8,def:[12,13,14,17,18,19,20,21],default_namespac:[5,17],default_worker_nam:[5,17],defaultserialis:[1,5,6,7,8,9],defer:14,defin:5,delai:[5,7,17,20],depend:14,depict:12,deploi:14,deserialis:[4,6],deserialise_args_and_kwarg:4,deserialise_request:4,deserialise_result:4,destin:[6,7],detail:5,develop:17,deviat:17,dict:9,dictionari:[8,9],differ:[7,12,13,19,21],direct:[20,21],directli:8,disabl:14,display_nam:[5,6],distribut:17,divid:[0,18],drop:6,dserialis:4,due:[13,14],dummy_context:21,dure:13,each:[6,12,14,17,19],effect:21,effort:14,egress:[5,7,8,9,12,14,17,21],egress_type_nam:[5,7,17],either:[13,18,19,20],elif:17,ellipsi:5,els:[7,18],emit:21,empti:6,enabl:[0,14],enable_inline_task:14,encapsul:8,ensur:14,enter:19,entri:[4,6,19],enumer:10,environ:14,error:[13,17],etc:13,event:[5,6,11,16],eventhandl:[1,5],exampl:[5,7,8,9,12,13,14,15,17,20,21],example_workflow:12,exce:14,except:[5,13,17,18,20],exclus:5,execut:13,exist:0,explic:4,exponential_back_off:20,expos:[5,12],extend:5,extens:[5,16],extern:14,extract:5,factori:9,fail:[5,13],failur:[5,18],fairli:17,fals:[5,6,13],favour:21,file:5,finally_act:6,finally_do:[6,18],find:21,fire:[0,6],first:5,fix:[5,19],flink:[4,5,6,7,8,9,12,13,15,18,19,20],flinktask:[1,17],flinktaskscli:[1,9,20],flinktasksclientfactori:[1,8],flow:13,fly:14,focu:17,folder:14,follow:18,forget:0,form:7,format:[4,7],friendli:[5,6],from:[4,5,6,7,13,14,15,17,21],from_proto:[4,6],fruit:[5,6,17,18,21],fun:[5,6],func:5,function_nam:5,functool:5,further:14,futur:8,gener:[13,14,17],get:8,get_address:7,get_caller_address:7,get_caller_id:[7,18],get_client:[8,9],get_destin:6,get_parent_task_address:7,get_parent_task_id:7,get_pipeline_address:7,get_pipeline_id:7,get_request:8,get_request_async:[0,8],get_result:8,get_result_async:[0,8],get_root_pipeline_address:7,get_root_pipeline_id:7,get_stat:19,get_statu:8,get_status_async:[0,8],get_task:[5,6],get_task_id:7,github:[15,16],given:[5,6,19],going:[13,21],good:[14,18],gpu:12,gpu_work:12,greater:13,grid:13,group:[6,8,9,17],group_id:8,grow:14,handl:8,handler:[6,11,13],has:[7,12,13,17,19,21],have:[17,18,21],hello:14,hello_world:14,help:[4,5,6,7,8,9,10,11],here:5,histor:17,hive:12,hold:14,how:21,ident:7,identifi:6,impact:[12,21],implement:[5,17],in_parallel:[6,14,17,18,19],includ:[5,13,18,21],incom:5,independ:19,indic:8,indirect:20,ingress:[6,8,9,12],inher:19,initi:[4,5,6,7,8,9,10,11,18],initial_cal:17,inline_task:14,input:[4,5,21],insid:[4,6],instal:15,instanc:[4,5,6,8],instanti:[5,8],instead:[5,13,21],interact:14,intermdi:17,interrupt:13,introduct:16,intuit:17,invalid:6,invoc:[4,20],invok:[6,13,20],involv:12,is_empti:6,is_fruit:[5,6],is_pipelin:[11,13],is_typ:17,isol:19,issu:[5,17],item:[4,5,14],iter:4,its:[12,19],kafka:[8,9,17],kafka_broker_url:[8,9],kafka_consumer_properti:[8,9],kafka_producer_properti:[8,9],kafka_properti:[8,9],kafkaconsum:[8,9],kafkaproduc:[8,9],keep:17,kei:19,know:21,known:4,known_proto_typ:4,kwarg:[4,5,6],lambda:5,larg:[5,14],last:6,later:0,len:[17,18],length:13,let:17,life:17,lightweight:17,like:[0,18,21],limit:[13,18],list:[6,8,9,17,18],listen:[8,9],load:17,load_timeseri:17,logic:[12,18,21],longer:17,look:21,mai:[0,5,12,14,18,19,20,21],make:[5,6,12,17],make_posit:18,manag:17,map:[5,8,9],market:17,max_parallel:[6,14,18],max_retri:20,maximum:6,mayb:21,mean:17,memois:19,memoised_multipli:19,memoiz:9,memori:[5,14],merg:4,merge_args_and_kwarg:4,messag:[4,5,6,7,17,20,21],method:[4,5,6,7,8,9,11],might:[17,21],modul:[5,12],module_nam:5,more:14,most:[7,14],mult_a:18,mult_b:18,multipl:[20,21],multipli:[0,5,18,20,21],multiply_and_subtract:18,must:14,mutat:21,name:[5,6,7,12,21],namespac:[5,6,7,12,19,21],necessari:5,nest:[7,18],next:14,non:[0,18],none:[4,5,6,7,8,9,13,17],normal:5,notebook:14,notify_pipeline_cr:11,notify_pipeline_finish:11,notify_pipeline_status_chang:11,notify_pipeline_task_finish:11,notify_task_finish:11,notify_task_retri:11,notify_task_start:11,now:21,number:[6,13,14,21],number_of_stock:17,object:5,off:12,old:17,on_pipeline_cr:[11,13],on_pipeline_finish:[11,13],on_pipeline_status_chang:[11,13],on_pipeline_task_finish:[11,13],on_task_finish:[11,13],on_task_retri:[11,13],on_task_start:[11,13],onc:14,one:[6,7,13,17],onli:[5,12,14],oper:0,optim:12,option:[4,5,6,7,8,9,14],orchestr:[12,13,17],order:5,ordinari:[17,21],origin:8,other:[6,12,17,21],otherwis:[4,5,6,21],our:[12,17],out:[0,16],outgo:21,output:21,outsid:[5,17],overrid:8,own:[5,7,12,18,19],pack:4,parallel:[5,6,14,18],param:5,paramet:[4,5,6,7,8,9,17,18,20],parent:7,part:7,partial:5,pass:[4,5,8,9,12,13,14,20],pattern:18,paus:[6,8,13,14],pause_pipelin:8,pause_pipeline_async:[0,8],payload:4,per:[8,9],perform:[0,12],permit:18,pick:[12,21],pickl:14,pip:15,pipelin:[4,5,6,7,8,11,12,14,16,17,19],pipeline_or_task:8,pipeline_proto:6,pipeline_st:13,pipeline_work:12,pipelinebuild:[1,5,8,19],pipelinest:13,piplin:0,place:[5,17,18],plain:17,point:[6,13],pointer:14,polici:[4,5,6],portfolio:17,possibl:4,pre:5,prefer:8,previou:[4,18],previous:8,price:17,print:17,print_exc:17,probabl:21,produc:[5,13],product:14,profession:17,properti:[5,6,7,8,9],proto:[4,13],protobuf:[4,5,6],protobuf_convert:4,provid:[4,5,14],purpos:13,put:18,pypi:15,python:[4,5,6,14,15,17,20],rais:[6,13],reach:13,read:13,realiti:12,reason:14,receiv:17,reciev:13,recommend:14,redi:5,regardless:18,regist:[5,12],register_builtin:5,repli:[8,9,21],reply_top:[8,9],repo:15,repres:5,represent:7,request:[4,7,8,9,12,14],request_top:[8,9],requir:[5,12],resourc:12,respons:[8,9],result:[4,5,6,8,12,13,14,17,18,19,20,21],resus:17,retri:[4,5,6,18,20],retry_count:[11,13],retry_for:20,retry_polici:[4,5,6,20],retrypolici:[5,13,20],revisit:17,root:7,run:[0,5,12,13,14,17],run_async:[5,17,20],run_pipelin:[5,6],s3_storag:14,same:[7,14,17,19,21],save:[0,5],scenario:13,schedul:0,sdk:7,sdkaddress:7,second:[5,20],secur:14,see:[4,5,6,7,8,9,10,11,15],self:[4,5,6,7,8,9,10,11,21],semphor:18,send:[0,5,6,7,12,14,17,18,19,20,21],send_egress:[17,21],send_egress_messag:7,send_messag:7,send_message_aft:7,send_result:5,sens:12,sent:12,seper:12,serialis:[4,5,6,7,8,9],serialise_args_and_kwarg:4,serialise_request:4,serialise_result:4,set:[5,6,7,8,9,14,18,19,20],set_stat:19,set_storage_backend:[5,14],setup:14,share:[18,19],should:[5,6,18,21],side:[14,18],signatur:[4,5,6,7,8,9,10,11],similarli:12,simpl:17,simpli:5,simplicti:21,simplist:12,sinc:[17,19],singl:[4,12,14,18,20],size:[13,14],solut:14,some:[0,7,13,15,17,21],sourc:16,spec:20,specifi:5,spin:17,standand:17,standard:17,state:[4,5,7,13,16,20],stateful_funct:21,stateful_multipli:21,statefulli:19,statefun:[5,12,15],statefun_task:[4,5,6,7,8,9,10,11,14],statu:[8,11],std:17,std_dev:17,step:18,stock:17,stock_list:17,storag:[5,14,17,21],storagebackend:[5,14],store:[14,17],str:[5,7,17,19],string:7,sub:12,sub_c:18,submit:[0,8,9,18,21],submit_async:[8,12,14,18,19,20],subscrib:8,subtract:[18,20],success:18,successfulli:8,suitabl:14,sum:18,suppli:7,support:18,system:17,target:7,target_id:7,task:[0,4,5,6,7,8,9,11,12,15],task_args_and_kwarg:4,task_except:[11,13],task_id:[5,6,19],task_input:5,task_nam:7,task_request:[4,5,7,11,13],task_result:[4,11,13],task_result_or_except:[4,11],task_typ:5,task_uid:7,task_using_context:18,taskactionrequest:[5,8],taskcontext:[1,5],taskexcept:4,taskrequest:[0,4,5,6,7,8,13],taskresult:[0,4,8],tasksexcept:13,taskstatu:[0,1],temporari:5,termin:18,test:[6,14,17,20,21],test_result:17,testabl:17,than:13,thei:[5,13,17,18,20],them:17,therefor:19,thi:[5,6,7,8,12,13,14,17,18],thread:8,threshold:14,through:21,thrown:20,time:13,timedelta:[7,20],timeseri:17,to_address_and_id:7,to_args_and_kwarg:4,to_pipelin:6,to_proto:[4,6],to_task:5,to_task_request:6,togeth:17,top:[5,7],topic:[7,8,9,12,17,21],traceback:17,track:13,trade:21,trigger:20,trivial:17,trust:14,tupl:[4,5,6],tupleofani:4,two:21,two_numbers_typ:21,type:[0,4,5,6,7,8,9,10,11,12,13,21],type_nam:5,typenam:7,typic:17,unchang:4,under:5,unhandl:18,union:[4,8],uniqu:[7,8,9,19],unit:21,unpack:[4,5],unpack_respons:4,unpack_task_request:5,unpaus:[0,8],unpause_pipelin:8,unpause_pipeline_async:[0,8],unreliable_task:20,unsabl:5,url:[8,9],use:[5,6,7,8,9,17],used:[5,8,9,13,18],useful:[13,14],using:[15,16,17,18,19,20],uuid4:19,valid:6,valu:[4,5,7,10,17,18],value_spec:20,valueerror:[6,20],variou:13,venu:13,via:14,vod:17,wait:[6,17],want:5,well:18,what:[14,21],when:[7,8,12,13,14,19],where:[12,13],whether:[5,6,8],which:[5,6,7,13,17,18],who:21,with_context:[5,18,19],with_stat:[5,18],within:18,without:[17,21],work:[12,14],worker:[1,5,6,7,8,9,12,13,14,17,20],worker_nam:[5,6,12,19],workflow:17,world:14,would:5,wrap:[5,17],wrapper:[5,7,18],write:[16,17],written:[19,21],yaml:[5,12],you:[5,14,18,21],your:[5,14]},titles:["Actions","API Reference","Worker API","Client API","DefaultSerialiser","FlinkTasks","PipelineBuilder","TaskContext","FlinkTasksClient","FlinkTasksClientFactory","TaskStatus","EventHandlers","Deployment Topologies","Events","Extensions","Getting Started","Flink Tasks","Introduction","Pipelines","Stateful Tasks","Tasks","Tasks vs Functions"],titleterms:{"function":[17,21],about:16,access:18,action:0,api:[1,2,3],call:20,cancel:0,chang:13,client:3,comparison:21,composit:18,condit:18,context:18,continu:18,control:0,creat:13,defaultserialis:4,deploy:12,error:[18,20],event:13,eventhandl:11,execut:18,extens:14,fan:18,finish:13,flink:[16,17,21],flinktask:5,flinktaskscli:8,flinktasksclientfactori:9,flow:0,get:15,handl:[18,20],inlin:14,introduct:17,motiv:17,offload:14,orchestr:18,out:18,pass:18,paus:0,pipelin:[0,13,18],pipelinebuild:6,python:21,queri:0,recurs:18,refer:1,regist:20,request:0,result:0,resum:0,retri:13,start:[13,15],state:[14,17,18,19,21],statu:[0,13],task:[13,14,16,17,18,19,20,21],taskcontext:7,taskstatu:10,topolog:12,what:17,worker:2}})