# Creator:      Sergii Butrymenko
# Dataset Name: Texas - State CC Dataset
# Task #:       32
# Migrated:     June 2021

# ruby mlc.rb --tool="clean::tx::texas_campaign_finance"
# ruby mlc.rb --tool="clean::tx::texas_campaign_finance" --mode='filer'
# ruby mlc.rb --tool="clean::tx::texas_campaign_finance" --mode='contributor'
# ruby mlc.rb --tool="clean::tx::texas_campaign_finance" --mode='payee'
# ruby mlc.rb --tool="clean::tx::texas_campaign_finance" --mode='city'

# require_relative "./_common.rb"

def execute(options = {})
  route_db01 = C::Mysql.on(DB01, 'usa_fin_cc_raw')

  table_description =
    {
      filer: {
        raw_table: 'texas_campaign_finance_filers_012020',
        clean_table: 'texas_campaign_finance__filernames_clean',
        fn_column: 'filernamefirst',
        ln_column: 'filernamelast',
        sf_column: 'filernamesuffixcd',
        org_column: 'filernameorganization',
        type_column: 'filerpersenttypecd',
        clean_column: 'clean_name',
      },
      contributor: {
        raw_table: 'texas_campaign_finance_contributions_012020',
        clean_table: 'texas_campaign_finance__contributornames_clean',
        fn_column: 'contributornamefirst',
        ln_column: 'contributornamelast',
        sf_column: 'contributornamesuffixcd',
        org_column: 'contributornameorganization',
        type_column: 'contributorpersenttypecd',
        clean_column: 'clean_name',
      },
      payee: {
        raw_table: 'texas_campaign_finance_expends_012020',
        clean_table: 'texas_campaign_finance__payeenames_clean',
        fn_column: 'payeenamefirst',
        ln_column: 'payeenamelast',
        sf_column: 'payeenamesuffixcd',
        org_column: 'payeenameorganization',
        type_column: 'payeepersenttypecd',
        clean_column: 'clean_name',
      },
      city: {
        raw_table: 'texas_campaign_finance_filers_012020',
        clean_table: 'texas_campaign_finance_cities',
        # fn_column: 'payeenamefirst',
        # ln_column: 'payeenamelast',
        # sf_column: 'payeenamesuffixcd',
        # org_column: 'payeenameorganization',
        # type_column: 'payeepersenttypecd',
        # clean_column: 'clean_name',
      },
    }

  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]

  case mode
  when :filer, :contributor, :payee
    names_cleaning(table_info, where_part, route_db01)
  when :city
    cities_cleaning(table_info, where_part, route_db01)
  else
    names_cleaning(table_description[:filer], where_part, route_db01)
    names_cleaning(table_description[:contributor], where_part, route_db01)
    names_cleaning(table_description[:payee], where_part, route_db01)
    cities_cleaning(table_description[:city], where_part, route_db01)
  end
  route_db01.close
end

def names_cleaning(table_info, where_part, route)
  last_clean_run_id = get_last_run_id(table_info[:clean_table], route)
  last_source_run_id = get_last_run_id(table_info[:raw_table], route)
  last_clean_run_id = 0 if last_clean_run_id.nil?
  # puts last_clean_run_id.class
  # puts last_clean_run_id
  # puts last_source_run_id.class
  # puts last_source_run_id
  message_to_slack("There is no any new names in *#{table_info[:raw_table]}* table...") unless last_source_run_id > last_clean_run_id
  new_names_ent, new_names_ind = get_new_names_list(table_info, last_clean_run_id, route)

  new_names_ent.each do |name|
    clean_name = name
    clean_name['clean_name'] = MiniLokiC::Formatize::Cleaner.org_clean(name[table_info[:org_column]].squeeze(' '))
    # puts JSON.pretty_generate(clean_name).yellow
    insert(route, table_info[:clean_table], clean_name, true, true)
  end

  new_names_ind.each do |name|
    clean_name = name
    clean_name['clean_name'] = MiniLokiC::Formatize::Cleaner.person_clean((name[table_info[:ln_column]] + ', ' + name[table_info[:fn_column]]).squeeze(' '))
    if name[table_info[:sf_column]] != ''
      suffix = name[table_info[:sf_column]].sub('EDD', 'EdD').sub('ESQ', 'Esq.').sub('JR', 'Jr.').sub('MED', 'MEd').sub('PHD', 'PhD').sub('RPH', 'RPh').sub('SR', 'Sr.')
      clean_name['clean_name'] = clean_name['clean_name'] + ' ' + suffix
    end
    # puts JSON.pretty_generate(clean_name).yellow
    insert(route, table_info[:clean_table], clean_name, true, true)
  end
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #32] Texas - State CC Dataset* \n>#{message}",
    as_user: true
  )
end

def get_last_run_id(table, route)
  query = <<~SQL
    SELECT MAX(run_id) AS run_id
    FROM #{table};
  SQL
  # puts query.yellow
  route.query(query).to_a.first['run_id']
end

def get_new_names_list(table_info, last_clean_run_id, route)
  query_filer_ent = <<~SQL.chomp
    \n  AND (t1.#{table_info[:org_column]} NOT LIKE '%DISSOLVED%' OR t1.#{table_info[:org_column]} NOT LIKE '%INACTIVE%')\
    \n  AND committeestatuscd NOT IN ('INACTIVE','TERMINATED')
  SQL
  query_filer_ind = <<~SQL.chomp
    \n  AND t1.#{table_info[:ln_column]} NOT LIKE '%DECEASED%'\
    \n  AND committeestatuscd NOT IN ('INACTIVE','TERMINATED')
  SQL
  query_entity = <<~SQL.chomp
    SELECT t1.#{table_info[:org_column]}, t1.#{table_info[:type_column]}, t1.run_id
    FROM #{table_info[:raw_table]} t1
      LEFT OUTER JOIN #{table_info[:clean_table]} t2
        ON (t1.#{table_info[:org_column]}=t2.#{table_info[:org_column]}
          AND t2.#{table_info[:type_column]}='ENTITY')
    #{if table_info[:clean_table] == 'texas_campaign_finance__filernames_clean'
        "WHERE t1.run_id>#{last_clean_run_id}"
      else
        "WHERE t1.run_id=(SELECT MAX(run_id) FROM #{table_info[:raw_table]})"
      end}
      AND filertypecd IN ('CEC', 'GPAC', 'JSPC', 'LEG', 'MPAC', 'PTYCORP', 'SPAC')
      AND t1.#{table_info[:type_column]}='ENTITY'\
      #{query_filer_ent if table_info[:clean_table] == 'texas_campaign_finance__filernames_clean'}
      AND t2.#{table_info[:org_column]} IS NULL;
  SQL
  query_individual = <<~SQL.chomp
    SELECT t1.#{table_info[:fn_column]}, t1.#{table_info[:ln_column]}, t1.#{table_info[:sf_column]}, t1.#{table_info[:type_column]}, t1.run_id
    FROM #{table_info[:raw_table]} t1
      LEFT OUTER JOIN #{table_info[:clean_table]} t2
        ON (t1.#{table_info[:fn_column]}=t2.#{table_info[:fn_column]}
          AND t1.#{table_info[:ln_column]}=t2.#{table_info[:ln_column]}
          AND t1.#{table_info[:sf_column]}=t2.#{table_info[:sf_column]}
          AND t2.#{table_info[:type_column]}='INDIVIDUAL')
    #{if table_info[:clean_table] == 'texas_campaign_finance__filernames_clean'
        "WHERE t1.run_id>#{last_clean_run_id}"
      else
        "WHERE t1.run_id=(SELECT MAX(run_id) FROM #{table_info[:raw_table]})"
      end}
      AND filertypecd IN ('CEC', 'GPAC', 'JSPC', 'LEG', 'MPAC', 'PTYCORP', 'SPAC')
      AND t1.#{table_info[:type_column]}='INDIVIDUAL'\
      #{query_filer_ind if table_info[:clean_table] == 'texas_campaign_finance__filernames_clean'}
      AND t2.#{table_info[:fn_column]} IS NULL
      AND t2.#{table_info[:ln_column]} IS NULL
      AND t2.#{table_info[:sf_column]} IS NULL;
  SQL

  if table_info[:clean_table] == 'texas_campaign_finance__contributornames_clean'
    receiveddt_part =
      if last_clean_run_id>0
        <<~SQL
          (SELECT MAX(receiveddt) 
           FROM #{table_info[:raw_table]}
           WHERE run_id=#{last_clean_run_id})
        SQL
      else
        "'2019-01-01'"
      end
    query_entity = <<~SQL.chomp
      SELECT #{table_info[:org_column]},
             #{table_info[:type_column]},
             run_id
      FROM #{table_info[:raw_table]}
      WHERE run_id=(SELECT MAX(run_id) FROM #{table_info[:raw_table]})
        AND filertypecd IN ('CEC', 'GPAC', 'JSPC', 'LEG', 'MPAC', 'PTYCORP', 'SPAC')
        AND #{table_info[:type_column]}='ENTITY'
        AND receiveddt>#{receiveddt_part};
    SQL

    query_individual = <<~SQL.chomp
      SELECT #{table_info[:fn_column]},
             #{table_info[:ln_column]},
             #{table_info[:sf_column]},
             #{table_info[:type_column]},
             run_id
      FROM #{table_info[:raw_table]}
      WHERE run_id=(SELECT MAX(run_id) FROM #{table_info[:raw_table]})
        AND filertypecd IN ('CEC', 'GPAC', 'JSPC', 'LEG', 'MPAC', 'PTYCORP', 'SPAC')
        AND #{table_info[:type_column]}='INDIVIDUAL'
        AND receiveddt>#{receiveddt_part};
    SQL
  end

  # puts query_entity.blue
  # puts query_individual.green
  [route.query(query_entity).to_a.uniq, route.query(query_individual).to_a.uniq]
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  str = str.to_s
  return str if str.nil? || str.empty?

  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def insert(db, tab, h, ignore = false, log=false)
  query = "INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')}) VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});"
  puts query.red if log
  db.query(query)
end

########################################### CITIES


TABLE_SRC_CITIES = 'texas_campaign_finance_filers_012020'
TABLE_CLN_CITIES = 'texas_campaign_finance_cities'


def get_new_cities_list(table_info, last_run_id, route)
  query = <<~SQL
    SELECT DISTINCT filerstreetcity AS filer_city,
      SUBSTRING_INDEX(filerstreetpostalcode, '-', 1) AS filer_zip,
      filerstreetstatecd AS filer_state,
      run_id
    FROM #{table_info[:raw_table]}
    WHERE filerstreetcity<>'' AND filerstreetpostalcode<>'' AND filerstreetstatecd='TX' AND run_id>#{last_run_id}
    UNION DISTINCT
    SELECT DISTINCT filermailingcity AS filer_city,
      SUBSTRING_INDEX(filermailingpostalcode, '-', 1) AS filer_zip,
      filermailingstatecd AS filer_state,
      run_id
    FROM #{table_info[:raw_table]}
    WHERE filermailingcity<>'' AND filermailingpostalcode<>'' AND filermailingstatecd='TX' AND run_id>#{last_run_id}
    ORDER BY filer_city, filer_zip;
  SQL
  # puts query
  route.query(query).to_a
end

def get_zip_list(route)
  query = <<~SQL
    SELECT zip, county
    FROM hle_resources_readonly_sync.zipcode_data
    WHERE state='TX';
  SQL
  # puts query
  route.query(query).to_a
end

def get_org_id_list(route)
  query = <<~SQL
    SELECT short_name AS city,
      county_name AS county,
      pl_production_org_id,
      pl_production_org_name
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching 
    WHERE state_name='Texas' 
    AND pl_production_org_id IS NOT NULL;
    # AND kind<>'CDP';
  SQL
  # puts query
  route.query(query).to_a
end

def cities_cleaning(table_info, where_part, route)
  last_run_id = get_last_run_id(table_info[:clean_table], route)
  last_run_id = 0 if last_run_id.nil?

  new_cities_list = get_new_cities_list(table_info, last_run_id, route)
  if new_cities_list.empty?
    message_to_slack("There are no new cities to clean! Exiting...")
    return
  end

  zip_list = get_zip_list(route)

  org_id_list = get_org_id_list(route)

  new_cities_list.each do |city|
    filer_county = zip_list.select{|el| el['zip'] == city['filer_zip'].to_i}
    city['filer_county'] = filer_county[0]['county'] if filer_county.count == 1
    city['county_status'] = filer_county.count

    org_id = org_id_list.select{|el| el['city'] == city['filer_city']}
    if org_id.count > 1
      org_id = org_id_list.select{|el| el['city'] == city['filer_city'] && el['county'] == city['filer_county']}
    end
    # puts org_id

    city['pl_production_org_id'] = org_id[0]['pl_production_org_id'] if org_id.count == 1
    city['org_id_status'] = org_id.count
    # puts JSON.pretty_generate(city).cyan
    insert(route, table_info[:clean_table], city, true)
  end
end
