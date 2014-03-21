kpi_ads
kpi_detail
kpi_direct_access
kpi_direct_region
kpi_domain
kpi_entry_page
kpi_heatmap
kpi_hotlink
kpi_out_link
kpi_out_region
kpi_page
kpi_region_distribute
kpi_respondomain
kpi_search_engines
kpi_search_region
kpi_search_word

create 'kpi_ads', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'loty', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_detail', {NAME => 'cf', VERSIONS => 1}
create 'kpi_page', {NAME => 'cf', VERSIONS => 1}
create 'kpi_respondomain', {NAME => 'cf', VERSIONS => 1}
create 'kpi_entry_page', {NAME => 'cf', VERSIONS => 1}
create 'kpi_domain', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'loty', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_heatmap', {NAME => 'cf', VERSIONS => 1}
create 'kpi_hotmap', {NAME => 'cf', VERSIONS => 1}
create 'kpi_hotlink', {NAME => 'cf', VERSIONS => 1}
create 'kpi_direct_access', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_direct_region', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_out_link', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_out_region', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_region_distribute', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_search_engines', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_search_region', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'kpi_search_word', [{NAME => 'cf', VERSIONS => 1}, {NAME => 'vis', VERSIONS => 1}]
create 'config_domain', [{NAME => 'total', VERSIONS => 1}, {NAME => 'pattern', VERSIONS => 1},{NAME => 'filter', VERSIONS => 1}]
create 'config_user' , 'cf' 
put 'config_user' , 'admin', 'cf:password' , 'admin'
put 'config_user' , 'admin', 'cf:usertype' , '1' 
