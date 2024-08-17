# Creator:      Sergii Butrymenko
# Dataset Name: Montana Campaign Finance Data Cleaning
# Task #:       68
# Migrated:     April 2022

# ruby mlc.rb --tool="clean::mt::mt_campaign_finance"
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cand_name'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cand_mail_address'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cand_phys_address'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cmte_name'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cmte_mail_address'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cmte_phys_address'
#
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cand_contributor'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cand_payee'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cmte_contributor'
# ruby mlc.rb --tool="clean::mt::mt_campaign_finance" --mode='cmte_payee'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    cand_name: {
      raw_table: 'montana_campaign_finance_candidate_detail',
      clean_table: 'montana_campaign_finance_candidate_detail__names_clean',
      raw_column: 'full_name',
      clean_column: 'full_name_clean',
    },
    cand_mail_address: {
      raw_table: 'montana_campaign_finance_candidate_detail',
      clean_table: 'montana_campaign_finance_candidate_detail__address_clean',
      raw_column: 'mailing_address',
    },
    cand_phys_address: {
      raw_table: 'montana_campaign_finance_candidate_detail',
      clean_table: 'montana_campaign_finance_candidate_detail__address_clean',
      raw_column: 'physical_address',
    },
    cmte_name: {
      raw_table: 'montana_campaign_finance_committee_detail',
      clean_table: 'montana_campaign_finance_committee_detail__names_clean',
      raw_column: 'committee_name',
      clean_column: 'committee_name_clean',
    },
    cmte_mail_address: {
      raw_table: 'montana_campaign_finance_committee_detail',
      clean_table: 'montana_campaign_finance_committee_detail__address_clean',
      raw_column: 'mailing_address',
    },
    cmte_phys_address: {
      raw_table: 'montana_campaign_finance_committee_detail',
      clean_table: 'montana_campaign_finance_committee_detail__address_clean',
      raw_column: 'physical_address',
    },

    cand_contributor: {
      raw_table: 'montana_campaign_finance_contributions_candidate',
      clean_table: 'montana_campaign_finance_contributions_candidate__contributors_clean',
      raw_column: 'contributor_name',
      clean_column: 'contributor_name_clean',
      type_column: 'name_type',
    },
    cand_payee: {
      raw_table: 'montana_campaign_finance_expenditure_candidate',
      clean_table: 'montana_campaign_finance_expenditure_candidate__payees_clean',
      raw_column: 'payee_name',
      clean_column: 'payee_name_clean',
      type_column: 'name_type',
    },
    cmte_contributor: {
      raw_table: 'montana_campaign_finance_contribution_committee',
      clean_table: 'montana_campaign_finance_contribution_committee__contributors_clean',
      raw_column: 'contributor_name',
      clean_column: 'contributor_name_clean',
      type_column: 'name_type',
    },
    cmte_payee: {
      raw_table: 'montana_campaign_finance_expenditure_committee',
      clean_table: 'montana_campaign_finance_expenditure_committee__payees_clean',
      raw_column: 'payee_name',
      clean_column: 'payee_name_clean',
      type_column: 'name_type',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :cand_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_ind_names(table_info, route)
    # uniq_committee(table_info, route)
    # recent_date = get_recent_date(table_info, route)
    # names_list = fill_cmte_table(table_info, recent_date, where_part, route)
    # clean_cmte_names(table_info, recent_date, where_part, route)
  when :cand_mail_address
    full_address_cleaning(table_info, where_part, route)
  when :cand_phys_address
    full_address_cleaning(table_info, where_part, route)
  when :cmte_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_cmte_names(table_info, route)
  when :cmte_mail_address
    full_address_cleaning(table_info, where_part, route)
  when :cmte_phys_address
    full_address_cleaning(table_info, where_part, route)

  when :cand_contributor, :cmte_contributor, :cand_payee, :cmte_payee
    # contributors_cleaning(table_info, where_part, route)

    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #68] Montana Campaign Finance Data Cleaning* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

# Cleaning

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    raw_column = table_info.key?(:raw_city) ? table_info[:raw_city] : table_info[:raw_column]
    clean_column = table_info.key?(:clean_city) ? table_info[:clean_city] : table_info[:clean_column]
    constraints = "UNIQUE (#{raw_column})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:clean_state]
      state = "#{table_info[:clean_state]} VARCHAR(2),"
      usa_adcp_matching_id = "usa_adcp_matching_id INT DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:clean_state]}, #{table_info[:raw_city]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{raw_column} VARCHAR(255) NOT NULL,
         #{clean_column} VARCHAR(255) DEFAULT NULL,
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def clean_cmte_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_list = route.query(query).to_a

  if names_list.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table.", :info
    return
  end
  names_list.each do |row|
    puts row
    clean_name = row
    puts clean_name[table_info[:raw_column]].to_s.cyan
    result_name = row[table_info[:raw_column]].dup.sub(/\bCTE\b/, 'Committee to Elect').gsub(/[“”]/, '"').gsub('’', "'").strip
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end

    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/ C\/O.+/i, '').sub('&amp;', '&').sub('&#39;', "'").sub(/\bCOM\b/i, 'Committee').sub(/\bST REP\b/i, 'State Representative').sub(/\bSTATE REP\b/i, 'State Representative').sub(/\bDEM\b/i, 'Democratic').sub(/\bREP\b/i, 'Republican').sub(/\bCO\b/i, 'County').sub(/\bEMPL\b/i, 'Employee').sub(/\bEMPLS\b/i, 'Employees').sub(/\bGOV\b/i, 'Government').sub(/\bLEG\b/i, 'Legislative').sub(/\bCAMP\b/i, 'Campaign').sub(/\bTWP\b/i, 'Township').sub(/\bPOL\b/i, 'Political').sub(/\bFED\b/i, 'Federation').sub(/\bWRKRS\b/i, 'Workers').sub(/\bAz\b/i, 'Arizona').strip.squeeze(' '))
    puts JSON.pretty_generate(result_name).yellow
    # insert_query = <<~SQL
    #     INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column_ct]}, #{table_info[:clean_column]}, scrape_date)
    #     VALUES ('#{escape(clean_name[table_info[:raw_column_rt]])}', '#{escape(result_name)}', '#{row['scrape_date']}');
    # SQL
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE (#{table_info[:clean_column]} IS NULL OR #{table_info[:clean_column]} = '')
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query.red
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.", :info)
end

# Candidate Cleaning

def fill_table(table_info, recent_date, where_part, route)
  #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
  # SELECT r.#{table_info[:raw_column]}, MIN(last_scrape_date) AS scrape_date
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND r.created_at >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in the source table"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_ind_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table.", :info
    return
  end
  names_to_clean.each do |row|
    clean_name = row
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(row[table_info[:raw_column]].dup.gsub(/[“”]/, '"').strip)
    skip_it = if clean_name[table_info[:raw_column]].match?(/\btest(er)?\b|test(account|upload)|n\/a/i)
                1
              else
                0
              end
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}', skip_it=#{skip_it}
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

# Contributor Cleaning

def clean_mixed_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table."
    return
  end
  det = MiniLokiC::Formatize::Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    # puts clean_name[table_info[:raw_column]].to_s.cyan
    result_name = row[table_info[:raw_column]].dup.strip.sub(/^(mr\. &amp; mrs\. |mr?[r|s]\.? )/i, '').sub(/\s+\./, '.').sub(/\.{2,}/, '.').sub(/,{2,}/, ',').gsub('&amp;', '&')
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end

    clean_name[table_info[:type_column]] = det.determine(result_name)

    result_name = clean_name[table_info[:type_column]] == 'Person' ? MiniLokiC::Formatize::Cleaner.person_clean(result_name) : MiniLokiC::Formatize::Cleaner.org_clean(result_name)

    if result_name.match?(/(FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)$/i)
      m = result_name.match(/(.+) (FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)/i).captures
      clean_name[table_info[:clean_column]] =
        if m[0].match?(/,/)
          m[1] + ' ' + m[0].match(/(.+), ?(.*)/).captures.reverse.join(' ')
        else
          m.join(' ')
        end
    else
      clean_name[table_info[:clean_column]] = result_name
    end

    # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_column]].match?(/[a-z]/i)
    # puts result_name.green
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}', #{table_info[:type_column]}='#{clean_name[table_info[:type_column]]}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

# def fill_city_table(table_info, recent_date, where_part, route)
#   query = <<~SQL
#     SELECT r.#{table_info[:raw_state]}, r.#{table_info[:raw_city]}, MIN(last_scrape_date) AS scrape_date
#     FROM #{table_info[:raw_table]} r
#       LEFT JOIN #{table_info[:clean_table]} cl
#         ON r.#{table_info[:raw_state]} = cl.#{table_info[:clean_state]}
#         AND r.#{table_info[:raw_city]} = cl.#{table_info[:raw_city]}
#     WHERE cl.#{table_info[:clean_city]} IS NULL
#       #{"AND last_scrape_date >= '#{recent_date}'" if recent_date && !where_part}
#       #{"AND #{where_part}" if where_part}
#     GROUP BY r.#{table_info[:raw_state]}, r.#{table_info[:raw_city]};
#   SQL
#   puts query.green
#   names_list = route.query(query).to_a
#   if names_list.empty?
#     message_to_slack("No new records for *#{table_info[:raw_city]}* column found in source tables", :info)
#   else
#     parts = names_list.each_slice(10_000).to_a
#     parts.each do |part|
#       insert_query = <<~SQL
#         INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:clean_state]}, #{table_info[:raw_city]}, scrape_date)
#         VALUES
#       SQL
#       part.each do |item|
#         insert_query << "('#{escape(item[table_info[:raw_state]])}','#{escape(item[table_info[:raw_city]])}','#{item[:scrape_date]}'),"
#       end
#       insert_query = "#{insert_query.chop};"
#       puts insert_query.red
#       route.query(insert_query)
#     end
#   end
# end
#
# def clean_cities(table_info, route)
#   query = <<~SQL
#     SELECT #{table_info[:raw_city]}, #{table_info[:clean_state]}
#     FROM #{table_info[:clean_table]}
#     WHERE #{table_info[:clean_city]} IS NULL;
#   SQL
#   puts query
#   city_list = route.query(query).to_a
#   if city_list.empty?
#     message_to_slack "There is no any new cities in *#{table_info[:clean_table]}* table.", :info
#     return
#   end
#   city_list.each do |item|
#     puts JSON.pretty_generate(item).cyan
#
#     city_name = item[table_info[:raw_city]].dup.split(/\b/)
#                   .map(&:capitalize).join.sub(/'S\b/,"'s").squeeze(' ')
#                   .sub(/\bhts.?\b/i, 'Heights')
#                   .sub(/\btwn?s?p.?\b/i, 'Township')
#                   .sub(/\bjct.?\b/i, 'Junction')
#                   .sub(/\bSPG\b/i, 'Springs')
#     puts city_name
#     cn = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name.dup, item[table_info[:clean_state]], 1) if city_name.length > 5
#     city_name = cn if cn
#
#     city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name))
#     usa_adcp_matching_id = get_usa_adcp_matching_id(item[table_info[:clean_state]], city_name, route)
#     puts "#{item[table_info[:raw_city]]} -- #{city_name} -- #{usa_adcp_matching_id}".yellow
#     puts JSON.pretty_generate(city_name).yellow
#
#     update_query = <<~SQL
#       UPDATE #{table_info[:clean_table]}
#       SET #{table_info[:clean_city]}='#{escape(city_name)}',
#           usa_adcp_matching_id = #{usa_adcp_matching_id.nil? ? "NULL" : "#{usa_adcp_matching_id}"}
#       WHERE #{table_info[:clean_city]} IS NULL
#         AND #{table_info[:raw_city]}='#{escape(item[table_info[:raw_city]])}'
#         AND #{table_info[:clean_state]}='#{escape(item[table_info[:clean_state]])}';
#     SQL
#     puts update_query.red
#     route.query(update_query)
#   end
# end
#
# def get_usa_adcp_matching_id(state_code, city, route)
#   query = <<~SQL
#     SELECT id, pl_production_org_id
#     FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
#     WHERE state_name=(SELECT name
#                     FROM hle_resources_readonly_sync.usa_administrative_division_states
#                     WHERE short_name='#{state_code}')
#       AND short_name='#{escape(city)}'
#       AND bad_matching IS NULL
#       AND has_duplicate=0
#       AND pl_production_org_id IS NOT NULL;
#   SQL
#   # puts query.green
#   res = route.query(query).to_a
#   if res.empty? || res.count > 1
#     nil
#   else
#     res.first['id']
#   end
# end

# # Address Cleaning
# def full_address_cleaning(table_info, where_part, route)
#   # recent_date = get_recent_date(table_info, route)
#   # fill_table(table_info, recent_date, where_part, route)
#   split_full_address(table_info, where_part, route)
#   # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
# end

################## ADDRESS ##########################
def full_address_cleaning(table_info, where_part, route)
  begin
    query = <<~SQL
      SELECT DISTINCT #{table_info[:raw_column]} AS raw_address
      FROM #{table_info[:raw_table]} s
        LEFT JOIN #{table_info[:clean_table]} a ON s.mailing_address=a.raw_address
      WHERE a.raw_address IS NULL
        AND mailing_address IS NOT NULL
        AND mailing_address<>''
        #{'AND #{where_part}' if where_part};
    SQL
    puts query.green
    address_list = route.query(query, symbolize_keys: true).to_a
  rescue Mysql2::Error
    query = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
              (id BIGINT(20) AUTO_INCREMENT PRIMARY KEY,
               raw_address VARCHAR(255) NOT NULL,
               street_address VARCHAR(255) DEFAULT NULL,
               city VARCHAR(50) DEFAULT NULL,
               state VARCHAR(50) DEFAULT NULL,
               zip VARCHAR(10) DEFAULT NULL,
               usa_adcp_matching_id BIGINT(20) DEFAULT NULL,
               fixed_manually BOOLEAN NOT NULL DEFAULT 0,
               skip_it BOOLEAN NOT NULL DEFAULT 0,
               created_at timestamp DEFAULT CURRENT_TIMESTAMP,
               updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
               UNIQUE (raw_address))
            DEFAULT CHARSET = `utf8mb4`
            COLLATE = utf8mb4_unicode_520_ci;
    SQL
    puts query.red
    route.query(query)
    retry
  end

  state_store = nil
  city_condition = nil
  state_name_by_code = get_state_name_by_code(route)

  return if address_list.empty?

  address_list.each do |row|
    puts "RAW:    #{row[:raw_address]}".cyan
    address_to_clean = row[:raw_address].sub(/(?<=\d)\D{,2}$/i, '')
    m = address_to_clean.match(/(?<address_city>.*?),?\s{1,}
                                (?<state_code>\w+)\s?
                                (?<zip>\d{5}?[-\s]?\d{,4}?$|N\/?A$)/ix)
    zip = m[3][0..4]
    state_code = m[2]
    # address_city = m[1].sub(/[,\s]*$/, '')
    address_city = m[1].sub(/[,\s]*(#{state_code})?$/i, '')
    state_name = state_name_by_code[state_code]
    next if state_name.nil?

    if state_store != state_name
      state_store = state_name
      city_list = get_city_list(state_store, route)
      # puts city_list
      city_condition = city_list.join('|')
      # puts row[:state].black.on_cyan
      # puts state_store.black.on_cyan
      puts city_condition
    end
    # puts state_name
    p address_city
    p address_city.sub(/\s(Ft.?)\s\w*$/i, 'Fort').sub(/\s(S(?:ain)?t)\s\w*$/i, 'St.')
    m = address_city.sub(/\b(Ft.?)(?=\s\w*$)/i, 'Fort').sub(/\b(S(?:ain)?t)(?=\s\w*$)/i, 'St.').match(/(?<street_address>.*?)\W*(?<city>#{city_condition})$/i)
    # m = '405 Fayette Pike, Montgomery, West Virginia 25136'.match(/(.*?)\W*(SMontgomery city|Montgomery|St. Paul|Salem city|Salem|Sandy city|Sandy)?\W*(West Virginia|WV)\W*(\d*-?\d+)\W*$/i)
    p m
    if m.nil?
      parts = address_city.split(' ')
      city = parts.pop
      if %w[City Creek Falls Springs Star Town].include?(city) || %w[Saint Silver].include?(parts.last)
        city = "#{parts.pop} #{city}"
      end
      street_address = parts.join(' ')
    else
      street_address = m[:street_address]
      city = m[:city]
    end
    puts "ADDRESS+CITY: #{address_city}".yellow
    # puts "ADDRESS: #{street_address}".yellow
    puts "ADDRESS: #{MiniLokiC::Formatize::Address.abbreviated_streets(street_address)}".yellow
    puts "CITY:    #{city}".yellow
    puts "STATE:   #{state_code}".yellow
    puts "ZIP:     #{zip}".yellow

    street_address = MiniLokiC::Formatize::Address.abbreviated_streets(street_address.strip)
    usa_adcp_matching_id, city = get_usa_adcp_matching_id_and_city(state_name, city, route)

    insert_query = <<~SQL
      INSERT INTO #{table_info[:clean_table]} (raw_address, street_address, city, state, zip, usa_adcp_matching_id)
      VALUES ('#{escape(row[:raw_address])}', '#{escape(street_address)}', '#{escape(city)}', '#{state_name}', '#{zip}', #{usa_adcp_matching_id});
    SQL
    puts insert_query
    route.query(insert_query)
  end
end

def get_state_name_by_code(route)
  query = <<~SQL
    SELECT name, short_name AS code
    FROM hle_resources_readonly_sync.usa_administrative_division_states
    WHERE pl_production_org_id IS NOT NULL ;
  SQL
  res = route.query(query, symbolize_keys: true).to_a
  list = {}
  res.each {|i| list[i[:code]] = i[:name]}
  list
end


def get_city_list(state, route)
  # state = 'District of Columbia' if state == 'Federal'
  query = <<~SQL
    SELECT DISTINCT short_name AS city
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name='#{state}' AND short_name NOT LIKE '%(%'
    UNION
    SELECT DISTINCT place_name AS city
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name='#{state}' AND place_name NOT LIKE '%(%';
  SQL
  route.query(query, symbolize_keys: true).to_a.flat_map(&:values)
end

def get_usa_adcp_matching_id_and_city(state, city, route)
  query = <<~SQL
    SELECT id, short_name
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name='#{state}'
      AND (short_name='#{escape(city)}' OR place_name='#{escape(city)}')
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query, symbolize_keys: true).to_a
  if res.empty? || res.count > 1
    ['NULL', city.split(/\b/).map(&:capitalize).join]
  else
    [res.first[:id], res.first[:short_name]]
  end
end

def final_run(route)
  query = <<~SQL
    SELECT id, street_address, state
    FROM fbi_nibrs_agencies__addresses_clean
    WHERE usa_adcp_matching_id IS NULL
    ORDER BY state;
  SQL
  address_list = route.query(query, symbolize_keys: true).to_a

  state_store = nil
  city_condition = nil

  return if address_list.empty?

  address_list.each do |row|
    next if row[:street_address].empty?

    puts "STATE:   #{row[:state]}".cyan
    puts "FULL:    #{row[:street_address]}".cyan
    if state_store != row[:state]
      state_store = row[:state]
      city_list = get_city_list(state_store, route)
      city_condition = city_list.join('|')
    end
    m = row[:street_address].match(/(.*?)\W*(#{city_condition})$/i)
    # m = '405 Fayette Pike, Montgomery, West Virginia 25136'.match(/(.*?)\W*(SMontgomery city|Montgomery|St. Paul|Salem city|Salem|Sandy city|Sandy)?\W*(West Virginia|WV)\W*(\d*-?\d+)\W*$/i)
    # puts /(.*?)\W*(#{city_condition})\W*(#{row[:state]}|#{row[:short_name]})\W*(\d*-?\d+)\W*$/i
    next if m.nil?
    # puts m.to_a.count
    # puts m.to_a
    street_address = m[1]
    city = m[2]
    puts "ADDRESS: #{street_address}".yellow
    # puts "ADDRESS: #{MiniLokiC::Formatize::Address.abbreviated_streets(street_address)}".yellow
    puts "CITY:    #{city}".yellow

    street_address = MiniLokiC::Formatize::Address.abbreviated_streets(street_address)
    usa_adcp_matching_id = get_usa_adcp_matching_id(row[:state], city, route)

    unless city.nil? || city.empty?
      update_query = <<~SQL
        UPDATE fbi_nibrs_agencies__addresses_clean
        SET street_address='#{escape(street_address.strip)}',
            city='#{escape(city)}',
            usa_adcp_matching_id=#{usa_adcp_matching_id}
# ,fixed_manually=2
        WHERE id=#{row[:id]};
      SQL
      puts update_query
      route.query(update_query)
    end
  end
end

##########################################
def fix_cities(route)
  query = <<~SQL
    SELECT id, street_address, state
    FROM fbi_nibrs_agencies__addresses_clean
    WHERE usa_adcp_matching_id IS NULL
      AND (city IS NULL OR city='')
      AND street_address<>'';
  SQL
  address_list = route.query(query, symbolize_keys: true).to_a

  state = nil

  return if address_list.empty?

  address_list.each do |row|
    next if row[:street_address].empty?

    if state != row[:state]
      state = row[:state]
      # city_list = get_city_list(state, route)
    end
    address_parts = row[:street_address].match(/(.*)\s+([\w]*)$/)
    next if address_parts.nil?

    city = if address_parts[2].length < 5
             nil
           else
             MiniLokiC::DataMatching::NearestWord.correct_city_name(address_parts[2], row[:state], 1)
           end
    street_address = if city.nil?
                       row[:street_address].chomp(',')
                     else
                       address_parts[1]
                     end

    usa_adcp_matching_id = get_usa_adcp_matching_id(row[:state], city, route) unless city.nil?

    puts "ADDRESS: #{row[:street_address]}".cyan
    puts "ADDRESS: #{street_address}".green
    puts "CITY:    #{city}".green
    puts "ADCP:    #{usa_adcp_matching_id}".green

    next if usa_adcp_matching_id.nil?

    update_query = <<~SQL
      UPDATE fbi_nibrs_agencies__addresses_clean
      SET street_address='#{escape(street_address.strip)}',
            city='#{escape(city)}',
            usa_adcp_matching_id=#{usa_adcp_matching_id}
# ,fixed_manually=3
        WHERE id=#{row[:id]};
    SQL
    # puts update_query
    # puts
    route.query(update_query)
  end
end