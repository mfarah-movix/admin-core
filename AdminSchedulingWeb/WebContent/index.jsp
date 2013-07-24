<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="ISO-8859-1"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<meta charset="utf-8">
	<title>Admin Scheduling</title>
	
	<!-- CSS -->
	<link href="css/demo_page.css" rel="stylesheet" type="text/css" />
	<link href="css/demo_table.css" rel="stylesheet" type="text/css" />      
	<link href="css/demo_table_jui.css" rel="stylesheet" type="text/css" />
	<link rel="stylesheet" type="text/css" href="css/DT_bootstrap.css">
	<!-- Scripts -->
	<script src="js/jquery.js"></script>
	<script src="js/jquery.dataTables.js" type="text/javascript"></script>
	<script src="js/dataTables.bootstrap.js" type="text/javascript"></script>
	
	<script type="text/javascript" src="fancybox/lib/jquery.mousewheel-3.0.6.pack.js"></script>
	<link rel="stylesheet" href="fancybox/source/jquery.fancybox.css?v=2.1.5" type="text/css" media="screen" />
	<script type="text/javascript" src="fancybox/source/jquery.fancybox.pack.js?v=2.1.5"></script>
	
	<script type="text/javascript" charset="utf-8">
		$(document).ready( function () {
			oTable = $('#example').dataTable({
				"sDom" : 'rtlp<"clear">',
				"sPaginationType": "bootstrap",
				"bAutoWidth": false, // Disable the auto width calculation 
				"aoColumns": [
					{ "sWidth": "10%" },
					{ "sWidth": "5%" },
					{ "sWidth": "20%" },
					{ "sWidth": "5%"},
					{ "sWidth": "10%", "bSortable" : false},
					{ "sWidth": "38%", "bSortable" : false},
					{ "sWidth": "12%", "bSortable" : false}
				]
			});

			$.extend( $.fn.dataTableExt.oStdClasses, {
			    "sWrapper": "dataTables_wrapper form-inline"
			} );
			
			$('#search-box').keyup(function() {
				oTable.fnFilter( $(this).val() );
			});

// 			$(".fancy").fancybox({
// 				maxWidth	: 1280,
// 				maxHeight  : 710,
// 				fitToView	: true,
// 				width		: '100%',
// 				height		: '70%',
// 				padding		: 0,
// 				autoSize	: true,
// 				closeClick	: true,
// 				openEffect	: 'elastic',
// 				closeEffect	: 'fade',
// 				afterClose	: function(){location.reload();}
// 			});
		});
		
	</script>
	
	<link rel="stylesheet" href="css/site.css">
	<link rel="stylesheet" href="css/pygments.css">
	<link rel="stylesheet" href="bootstrap/css/bootstrap.css">
	<link rel="stylesheet" href="css/font-awesome.min.css">
	
	<style>
	        .header{
	            height:70px;
	            background-color:#691760;
	            color: white;
	            font-size:55pt;
	            padding:40px 0px 0px 30px;
	        }
	
	        .myFooter{
	            height:20px;
	            background-color:#691760;
	            color: white;
	            font-size:10pt;
	            padding:5px;
	            text-align:center;
	        }
	    </style>
</head>
<body>

<div class="navbar navbar-inverse navbar-static-top hidden-print">
    <div class="navbar-inner">
        <ul class="nav">
            <li><a href="http://admin.movixla.com/sp/">SP</a></li>
            <li class="active"><a href="#">Scheduling</a></li>
        </ul>
        <ul class="nav pull-right">
            <li><a href="mailto:dave@fontawesome.io"><i class="icon-user"></i>&nbsp; jperez</a></li>
        </ul>
    </div>
</div>

<div class="header">Scheduling / SUS</div>

<div class="navbar navbar-inverse navbar-static-top">
    <div class="navbar-inner">
        <ul class="nav">
            <li class="active"><a href="events">Eventos</a></li>
        </ul>

        <form class="navbar-search pull-right">
            <input id="search-box" type="text" class="search-query"  placeholder="Buscar...">
        </form>

        <div class="pull-right">
        	<a href="events?action=add" data-fancybox-type="iframe" class="btn btn-primary fancy"> <i class="icon-plus icon-white"></i> Nuevo </a>
        </div>
    </div>
</div>

<table cellpadding="0" cellspacing="0" border="0" class="table table-striped table-bordered" id="example">
	<thead>
		<tr>
	        <th>Operador</th>
	        <th>Producto</th>
	        <th>SP</th>
	        <th>Tipo</th>
	        <th>Días</th>
	        <th>Horario UTC</th>
	        <th>Acciones</th>
		</tr>
	</thead>
	<tbody>
		<c:forEach var="event" items="${events}">
		<tr class="odd_gradeA">
	        <td>
	        	<c:set var="opCountry" value="${event.getOperador().getPais().getCodigo().toUpperCase()}" />
	        	<c:set var="opName" value="${event.getOperador().name().split('_')[0].substring(0,1).toUpperCase().concat(event.getOperador().name().split('_')[0].substring(1).toLowerCase())}" />
	        	<c:if test="${opCountry == 'CO' && opName == 'Comcel'}">
	        		<c:set var="opCountry" value="CO" />
	        		<c:set var="opName" value="Claro" />
	        	</c:if>
	        	<c:if test="${opCountry == 'PA' && opName == 'Cableandwireless'}">
	        		<c:set var="opCountry" value="PA" />
	        		<c:set var="opName" value="C&W" />
	        	</c:if>
	        	<span class="label label-info">${opCountry}</span>&nbsp;${opName}
	        </td>
			<td>${event.getProducto()}</td>
	        <td>${event.getSp() == "" ? "*" : event.getSp()}</td>
	        <td>${event.getTipo() }</td>
	        <td>
	        	<ul style="list-style-type: none; padding: 0;margin: 0;">
		        <c:forEach var="dia" items="${event.getDias()}">
		        	<li>
		        	<c:if test="${dia.key == 'LU,MA,MI,JU,VI,SA,DO'}">LU a DO</c:if>
		        	<c:if test="${dia.key == 'LU,MA,MI,JU,VI'}">LU a VI</c:if>
		        	<c:if test="${dia.key != 'LU,MA,MI,JU,VI' && dia.key != 'LU,MA,MI,JU,VI,SA,DO'}">${dia.key}</c:if>
		        	</li>
		        </c:forEach>
	        	</ul>
	        </td>
	        <td>
	        	<ul style="list-style-type: none; padding: 0;margin: 0;">
		        <c:forEach var="dia" items="${event.getDias()}">
		        	<li>${dia.value}</li>
		        </c:forEach>
	        	</ul>
	        </td>
	        <td class="center">
		        <div class="btn-group">
		        	<a class="btn fancy" data-fancybox-type="iframe" href="events?action=add&eventId=${event.getId()}"><i class="icon-copy"></i></a>
<%-- 		        	<a class="btn" href="events?action=delete&eventId=${event.getId()}"><i class="icon-remove"></i></a> --%>
<!-- 		        	<a class="btn disabled" href="#" ><i class="icon-book"></i></a>  -->
		        	<a class="btn btn-primary fancy" data-fancybox-type="iframe" href="events?action=edit&eventId=${event.getId()}"><i class="icon-edit"></i></a>
		        </div>
	        </td>
		</tr>
		</c:forEach>
		
	</tbody>
</table>
<div class="myFooter">
   Movix - 2013
</div>

</body>
</html>