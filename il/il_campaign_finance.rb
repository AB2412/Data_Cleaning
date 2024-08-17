# Creator:      Sergii Butrymenko
# Dataset Name: Illinois State Campaign Finance
# Task #:       67
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/383
# Data set:     https://lokic.locallabs.com/data_sets/16
# Migrated:     August 2022

# ruby mlc.rb --tool="clean::il::il_campaign_finance"
# ruby mlc.rb --tool="clean::il::il_campaign_finance" --mode='contributor' --where="rcv_date >= '2016'"
# ruby mlc.rb --tool="clean::il::il_campaign_finance" --mode='vendor'
# ruby mlc.rb --tool="clean::il::il_campaign_finance" --mode='committee'
# ruby mlc.rb --tool="clean::il::il_campaign_finance" --mode='expenditure_purpose' --where="rcv_date >= '2016'"

def execute(options = {})
  route = C::Mysql.on(DB01, 'voters_2016')
  table_description = {
    contributor: {
      raw_table: 'hyperlocal_new_cc_contributions',
      clean_table: 'hyperlocal_new_cc_contributors',
      raw_name: 'contributed_by',
      clean_name: 'contributed_by_clean',
      name_type: 'contributor_type',
      entity_id: 'contributor_id',
      state: 'state',
      raw_city: 'city',
      clean_city: 'city_clean',
      raw_zip: 'zip',
      clean_zip: 'zip5',
      raw_address1: 'address1',
      raw_address2: 'address2',
      clean_address: 'address_clean',
    },
    vendor: {
      raw_table: 'hyperlocal_new_cc_committee_expenditures',
      clean_table: 'hyperlocal_new_cc_vendors',
      raw_name: 'vendor',
      clean_name: 'vendor_clean',
      name_type: 'vendor_type',
      entity_id: 'vendor_id',
      state: 'state',
      raw_city: 'city',
      clean_city: 'city_clean',
      raw_zip: 'zip',
      clean_zip: 'zip5',
      raw_address1: 'address1',
      raw_address2: 'address2',
      clean_address: 'address_clean',
    },
    committee: {
      raw_table: 'hyperlocal_new_cc_committees',
      # raw_table: 'hyperlocal_new_cc_committees_TEST',
      # clean_table: 'hyperlocal_new_cc_committee_expenditures__purpose_clean',
      raw_column: 'cmte_name',
      clean_column: 'cmte_name_cleaned'
    },
    expenditure_purpose: {
      raw_table: 'hyperlocal_new_cc_committee_expenditures',
      clean_table: 'hyperlocal_new_cc_committee_expenditures__purpose_clean',
      raw_column: 'purpose',
      clean_column: 'purpose_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :contributor
    recent_date = get_recent_date(table_info, route)
    fill_name_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
    clean_city_and_address(table_info, route)
    set_similar_to(table_info, route)
    set_entity_id(table_info, route)
  when :vendor
    recent_date = get_recent_date(table_info, route)
    fill_name_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
    clean_city_and_address(table_info, route)
    set_similar_to(table_info, route)
    set_entity_id(table_info, route)
  when :expenditure_purpose
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_expenditures(table_info, route)
  when :committee
    update_new_cmte_names_from_exist(table_info, route)
    clean_cmte_names(table_info, route)
    # suggest_similar_committees(table_info, route)
    check_unmatched(table_info, route)
  # when :cand_phys_address
  #   full_address_cleaning(table_info, where_part, route)
  # when :cmte_mail_address
  #   full_address_cleaning(table_info, where_part, route)
  # when :cmte_phys_address
  #   full_address_cleaning(table_info, where_part, route)
  #
  # when :cand_contributor, :cmte_contributor, :cand_payee, :cmte_payee
  #   # contributors_cleaning(table_info, where_part, route)
  #
  #   recent_date = get_recent_date(table_info, route)
  #   fill_table(table_info, recent_date, where_part, route)
  #   clean_mixed_names(table_info, route)
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
    text: "*[CLEANING #67] Illinois State Campaign Finance* \n>#{type} #{message}",
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
    if table_info[:clean_table].include?('contributors') || table_info[:clean_table].include?('vendors')
      message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Create it manually. Exiting...")
      exit 1
    else
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
           created_by VARCHAR(255) DEFAULT 'Sergii Butrymenko',
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
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_name_table(table_info, recent_date, where_part, route)
  #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
  #     LEFT JOIN #{table_info[:clean_table]} t
  #       ON s.contributor_id=t.id
  query = <<~SQL
    SELECT #{table_info[:raw_name]},
      #{table_info[:raw_address1]},#{table_info[:raw_address2]},#{table_info[:raw_city]},#{table_info[:state]},#{table_info[:raw_zip]},
      MIN(created_at) AS scrape_date
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:entity_id]} IS NULL
      AND #{table_info[:raw_name]} IS NOT NULL
      AND #{table_info[:raw_name]} <> ''
      AND active=1
      #{"AND #{where_part}" if where_part}
    GROUP BY #{table_info[:raw_name]}, #{table_info[:raw_address1]}, #{table_info[:raw_address2]}, #{table_info[:raw_city]}, #{table_info[:state]}, #{table_info[:raw_zip]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in the source table"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_name]}, #{table_info[:raw_address1]}, #{table_info[:raw_address2]}, #{table_info[:raw_city]}, #{table_info[:state]}, #{table_info[:raw_zip]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_name]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_name]])}', '#{escape(item[table_info[:raw_address1]])}', '#{escape(item[table_info[:raw_address2]])}', '#{escape(item[table_info[:raw_city]])}', '#{escape(item[table_info[:state]])}', '#{escape(item[table_info[:raw_zip]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

# Contributor Cleaning

def clean_mixed_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_name]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_name]} IS NULL
    LIMIT 10000;
  SQL
  puts query.green
  cleaned = false
  det = MiniLokiC::Formatize::Determiner.new

  until cleaned
    names_to_clean = route.query(query).to_a
    if names_to_clean.empty?
      message_to_slack "There is no any new _#{table_info[:raw_name]}_ in *#{table_info[:clean_table]}* table."
      cleaned = true
    else
      names_to_clean.each do |row|
        # puts row
        clean_name = row

        result_name = row[table_info[:raw_name]].dup.strip.sub(/^(mr\. &amp; mrs\. |mr?[r|s]\.? )/i, '').sub(/\s+\./, '.').sub(/\.{2,}/, '.').sub(/,{2,}/, ',').gsub('&amp;', '&').sub(/,+$/, '')
        result_name = result_name.split(/,?\s+c\/o/i).first.gsub(/(?<=\b[a-z])('\s)(?=[a-z]{2,})/i, "'")
        while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
          result_name = result_name[1..-1]
        end

        # puts result_name
        # puts result_name.match?(%r(.+,.+\s*(&|and|\/)\s*\b)i)
        # puts result_name.sub(/[js]r\.?,/i, '').count(',') == 1
        # puts !result_name.match?(Regexp.new(BUSINESS.dup.to_s.gsub('|and|', '|').sub('|&|', '|')))
        # puts !result_name.match?(BUSINESS_SUFFIXES)

        if table_info[:raw_name] != 'vendor'
          is_couple = result_name.match?(%r(.+,.+\b\.?\s*(&|and|or|\/)\s*\b)i) &&
            (result_name.count(',') == 1 || result_name.count(',') == 2 && result_name.split(' ')[1]&.match(PERSON)) &&
            # result_name.count(/\b\s*(&|and|\/)\s*\b/i) == 1 &&
            !result_name.match?(Regexp.new(MiniLokiC::Formatize::BUSINESS.dup.to_s.gsub('|and|', '|').sub('|\s*&\s*|', '|').sub('|&|', '|'))) &&
            !result_name.match?(BUSINESS_SUFFIXES) &&
            !result_name.match?(/\d{2}/)
        else
          is_couple = false
        end
        # name_is_couple(result_name)

        # puts clean_name[table_info[:raw_name]].to_s.cyan
        # puts result_name.to_s.cyan
        # puts is_couple
        # puts BUSINESS
        # puts BUSINESS_SUFFIXES

        if is_couple
          clean_name[table_info[:name_type]] = 'Couple'
          # clean_name[table_info[:clean_name]] = MiniLokiC::Formatize::Cleaner.person_clean(result_name.sub(/\b\s*[&\/]\s*\b/, ' and ')).gsub(' And ', ' and ')
          parts = result_name.gsub(/\s*[&\/]\s*\b/, ' and ').split(',')
          clean_name[table_info[:clean_name]] = "#{MiniLokiC::Formatize::Cleaner.org_clean(parts.last.downcase)} #{MiniLokiC::Formatize::Cleaner.person_clean(parts[0...-1].join(' '), false)}"
          clean_name[table_info[:clean_name]].gsub!(' And ', ' and ')
          clean_name[table_info[:clean_name]].gsub!(' Or ', ' or ')
          clean_name[table_info[:clean_name]].gsub!(/(\b[a-z])(?=[\b\s])/i, '\1.')
          clean_name[table_info[:clean_name]].gsub!(' a. ', ' A. ')
        else
          clean_name[table_info[:name_type]] = det.determine(result_name)

          result_name = clean_name[table_info[:name_type]] == 'Person' ? MiniLokiC::Formatize::Cleaner.person_clean(result_name) : MiniLokiC::Formatize::Cleaner.org_clean(result_name)

          if result_name.match?(/(FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)$/i)
            m = result_name.match(/(.+)(?:\s|-|\/)(FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)/i).captures
            clean_name[table_info[:clean_name]] =
              if m[0].match?(/,/)
                "#{m[1]} #{m[0].match(/(.+), ?(.*)/).captures.reverse.join(' ')}"
              else
                m.join(' ')
              end
          else
            clean_name[table_info[:clean_name]] = result_name
          end
        end

        # clean_name[table_info[:clean_name]] = result_name.split(/,?\s+c\/o/i).first

        # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_name]].match?(/[a-z]/i)
        # puts result_name.green
        # puts JSON.pretty_generate(clean_name).yellow
        update_query = <<~SQL
          UPDATE #{table_info[:clean_table]}
          SET #{table_info[:clean_name]}='#{escape(clean_name[table_info[:clean_name]])}', #{table_info[:name_type]}='#{clean_name[table_info[:name_type]]}'
          WHERE id=#{clean_name['id']};
        SQL
        # puts update_query.red
        route.query(update_query)
      end
    end
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

def name_is_couple(name)
  if name.match?(/\b(co(rp)?|Federation|inc|Government|LLC|Mail|Marketing|Municipal|Print)\b/i)
    true
  else
    false
  end
end

# Clean expenditures

def clean_expenditures(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  items_list = route.query(query).to_a

  if items_list.empty?
    message_to_slack "There is no any new expenditure purpose in #{table_info[:clean_table]} table.", :info
    return
  end
  items_list.each do |row|
    puts row
    purpose = row[table_info[:raw_column]].dup
    puts purpose

    if (purpose.size <= 2 && purpose.downcase != 'ad' && !purpose.empty?) || !purpose.match?(/[a-z]/i)
      clean_purpose = 'Unspecified Expenses'
    else
      clean_purpose =
        case purpose
        when nil
          'Unspecified Expenses'
        when ''
          'Unspecified Expenses'
        when /\berrors?\b/i
          'Improper Payments'
        when /\b(ads?|advertising|banner|billboards?|books?|brochures|digital|lawn signs|masks|parade|posters?|promotional|t-shirts|tv|yard signs)\b/i
          'Advertisement'
        when /\b(auto|car|fuel|mileage|parking|taxi)\b/i
          'Vehicle Expenses'
        when /\b(cleaning|window)\b/i
          'Cleaning Services'
        when /\bcampaign\b/i
          'Campaign Expenses'
        when /\b(advocacy|attorney|legal|notary)\b/i
          'Legal Fees & Expenses'
        when /\b(air fare|hotel|lodging|travel)\b/i
          'Travel Expenses'
        when /\b(auction|raffle)\b/i
          'Auction Items & Supplies'
        when /\bbank\b/i
          'Financial Expenses'
        when /\b(donation|dues)\b/i
          'Donations'
        when /\belection\b/i
          'Election Expenses'
        when /\belectric(ity)?\b/i
          'Electric Service'
        when /\bequipment\b/i
          'Equipment, Rental'
        when /\b(christmas|dining|dinner|dj|event|gala|holiday|tickets|tix|xmas)\b/i
          'Event Expenses'
        when /\bfund(raiser|raising)?\b/i
          'Fundraising Expenses'
        when /\bgas(oline)?\b/i
          'Gasoline Expenses'
        when /\bgifts?\b/i
          'Gifts'
        when /\bgolf\b/i
          'Golf Outing Expenses'
        when /\bhealth\b/i
          'Health Insurance Expense'
        when /\b(Act\s?Blue|cable|calls|cell|communications|copie[rs]|copy|data(base)?|domain|graphic|ink|intern( services)?|internet|messenger|office|organiz(ation|er)|pens|printing|software|supplies|support|tech(nical|nology)?|(tele)?phone|web)\b/i
          'Technology & Office Expenses'
        when /IT/
          'Technology & Office Expenses'
        when /\b(media|telecom(munications)?|teleconference|video)\b/i
          'Media Production'
        when /\b(breakfast|catering|drinks|entertainment|flowers|food|lunch|meals|pizza|refreshments)\b/i
          'Meals & Entertainment'
        when /\b(marketing|pr)\b/i
          'Marketing'
        when /\b(e?mail(ing)?|envelopes|post(age)?|shipping|stamps)\b/i
          'Mail & Postage'
        when /\bmeeting\b/i
          'Meeting Expenses'
        when /\bmembership\b/i
          'Membership Dues'
        when /\bmerchan(dise|t)\b/i
          'Merchant Expenses'
        when /\bpayroll\b/i
          'Payroll Expenses'
        when /\b(photo(copies|graphs|graphy|s)?)\b/i
          'Media Production'
        when /\bprincipal\b/i
          'Principal Payment'
        when /\bstaff\b/i
          'Staff'
        when /\b(rent|utilities)\b/i
          'Rent & Utilities'
        when /\b(card|credit)\b/i
          'Credit Card Fees & Expenses'
        when /\bcontribut(e|ion)\b/i
          'Contribution'
        when /\bconsult(ant|ing)\b/i
          'Consulting'
        when /\btax(es)?\b/i
          'Taxes'
        when /\bfield\b/i
          'Field Work'
        when /\binsurance\b/i
          'Insurance'
        when /\b(collection|conference|e-?commerce|lobbyist|registration|transaction|wire transfer|witness)\b/i
          'Fees'
        when /\bgeneral\b/i
          'General Contribution'
        when /\bprimary\b/i
          'Primary Contribution'
        else
          'Other Expenses'
        end
    end
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_purpose)}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(row[table_info[:raw_column]])}';
    SQL
    puts update_query.red
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.", :info)
end

def update_new_cmte_names_from_exist(table_info, route)
  query = <<~SQL
    UPDATE #{table_info[:raw_table]} new
      JOIN #{table_info[:raw_table]} old
        ON new.#{table_info[:raw_column]}=old.#{table_info[:raw_column]}
          AND old.#{table_info[:clean_column]}<>''
          AND old.#{table_info[:clean_column]} IS NOT NULL
    SET new.#{table_info[:clean_column]}=old.#{table_info[:clean_column]}
    WHERE new.#{table_info[:clean_column]}='' OR new.#{table_info[:clean_column]} IS NULL;
  SQL
  puts query.red
  route.query(query)
end

def clean_cmte_names(table_info, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_column]}
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:raw_column]} <> ''
      AND (#{table_info[:clean_column]} IS NULL OR #{table_info[:clean_column]}='');
  SQL
  puts query.green
  names_list = route.query(query).to_a

  if names_list.empty?
    message_to_slack "There is no any new name in #{table_info[:raw_column]} table.", :info
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

    result_name = MiniLokiC::Formatize::Cleaner.
      org_clean(result_name.sub(/ C\/O.+/i, '').
        gsub('&amp;', '&').
        gsub(/-{2,}/, '-').
        gsub('&#39;', "'").
        gsub(/"([a-z])/){"\"#{Regexp.last_match[1].upcase}"}.
        gsub(/\bO(")[a-z]/, "''").
        gsub(/\b50w\b/i, '50W').
        gsub(/(?<!\.)\bCOM\b/i, 'Committee').
        gsub(/\bDEM\b/i, 'Democratic').
        gsub(/\bDollyVole\b/i, '"Dolly" Vole').
        gsub(/\b"phil"carlson\b/i, '"Phil" Carlson').

        gsub(/\bST(ATE)? REP\b/i, 'State Representative').
        gsub(/\bREP\b/i, 'Republican').
        gsub(/\bCO\b/i, 'County').
        gsub(/\bEMPL\b/i, 'Employee').
        gsub(/\bEMPLS\b/i, 'Employees').
        gsub(/\bGOV\b/i, 'Government').
        gsub(/\bLEG\b/i, 'Legislative').
        gsub(/\bCAMP\b/i, 'Campaign').
        gsub(/\bTWP\b/i, 'Township').
        gsub(/\bPOL\b/i, 'Political').
        gsub(/\bFED\b/i, 'Federation').
        gsub(/\bWRKRS\b/i, 'Workers').

        gsub(/\bBCKH\b/i, 'BCKH').
        gsub(/\bACT\b/i, 'ACT').
        gsub(/\bBIPO\b/i, 'BIPO').
        gsub(/\bCLASS\b/i, 'CLASS').
        gsub(/\bCPHIR\b/i, 'CPHIR').
        gsub(/\bCROPAC\b/i, 'CROPAC').
        gsub(/\bCUTS\b/i, 'CUTS').
        gsub(/\bCW\b/i, 'CW').
        gsub(/\bEPAC\b/i, 'EPAC').
        strip.squeeze(' '))
    puts JSON.pretty_generate(result_name).yellow
    # insert_query = <<~SQL
    #     INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column_ct]}, #{table_info[:clean_column]}, scrape_date)
    #     VALUES ('#{escape(clean_name[table_info[:raw_column_rt]])}', '#{escape(result_name)}', '#{row['scrape_date']}');
    # SQL
    update_query = <<~SQL
      UPDATE #{table_info[:raw_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE (#{table_info[:clean_column]} IS NULL OR #{table_info[:clean_column]} = '')
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
  text = <<~HEREDOC
    Table *#{table_info[:raw_table]}* was updated. #{names_list.size} committee names were cleaned and should be checked with query:
      SELECT #{table_info[:raw_column]}, #{table_info[:clean_column]} FROM #{table_info[:raw_table]} WHERE updated_at>='#{Date.today}';
    and matched
  HEREDOC
  message_to_slack(text, :warning)
end

def check_unmatched(table_info, route)
  query = <<~SQL
    SELECT COUNT(*) AS total_unmatched
    FROM #{table_info[:raw_table]}
    WHERE cf_pl_production_org_id IS NULL
      AND state = 'IL'
      AND site_committee_id IN
          (SELECT DISTINCT site_committee_id
           FROM hyperlocal_new_cc_contributions
           UNION
           SELECT DISTINCT committeeid
           FROM hyperlocal_new_cc_committee_expenditures);
  SQL
  total_unmatched = route.query(query).to_a.first['total_unmatched']
  if total_unmatched.zero?
    message_to_slack("There is no unmatched committees in *#{table_info[:raw_table]}* table.")
  else
    message_to_slack("#{total_unmatched} committees in *#{table_info[:raw_table]}* table should be matched.", :warning)
  end
end

def suggest_similar_committees(table_info, route)
  query = <<~SQL
    SELECT site_committee_id, cmte_name_cleaned
    FROM #{table_info[:raw_table]}
    WHERE cf_pl_production_org_id IS NOT NULL
      AND state = 'IL';
  SQL
  matched_list = route.query(query, symbolize_keys: true).to_a
  query = <<~SQL
    SELECT site_committee_id, cmte_name_cleaned
    FROM #{table_info[:raw_table]}
    WHERE cf_pl_production_org_id IS NULL
      AND state = 'IL';
  SQL
  list_to_match = route.query(query, symbolize_keys: true).to_a
  iter = 1
  list_to_match.each do |cmte_to_check|
    ranking = []
    puts cmte_to_check
    matched_list.each do |cmte|
      ranking <<
        {DIST: DamerauLevenshtein.distance(cmte_to_check[:cmte_name_cleaned], cmte[:cmte_name_cleaned]),
         NAME: cmte[:cmte_name_cleaned],
        CMID: cmte[:site_committee_id]}

    end
    puts ranking.sort {|a, b| a[:DIST] <=> b[:DIST]}[0..4]
    # p ranking.sort {|a, b| a.last <=> b.last}[0..4]
    if iter < 10
      iter += 1
    else
      break
    end
  end
  # if total_unmatched.zero?
  #   message_to_slack('There is no unmatched committees in *ca_campaign_finance_committees__matched* table.')
  # else
  #   message_to_slack("#{total_unmatched} committees in *ca_campaign_finance_committees__matched* table should be matched.", :warning)
  # end
end

# # Candidate Cleaning

def fill_table(table_info, recent_date, where_part, route)
  #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
  # SELECT r.#{table_info[:raw_column]}, MIN(last_scrape_date) AS scrape_date
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND r.created_at >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
      # AND r.#{table_info[:raw_column]} IS NOT NULL
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

def clean_city_and_address(table_info, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:state]}, #{table_info[:raw_city]}, #{table_info[:raw_zip]}, #{table_info[:raw_address1]}, #{table_info[:raw_address2]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_address]} IS NULL;
  SQL
  address_list = route.query(query).to_a
  return nil if address_list.empty?

  address_list.each do |item|
    puts JSON.pretty_generate(item).green
    record_data = item.dup
    # city_data[table_info[:clean_column]] = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    # city_name = prepare_city(city_data[table_info[:raw_column]]).split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    city_name = record_data[table_info[:raw_city]]
    city_name = city_name.split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s") unless city_name.empty?
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, record_data[table_info[:state]], 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city_name = correct_city_name
    end
    puts city_name.black.on_red

    raw_address = item[table_info[:raw_address1]]
    raw_address += ", #{item[table_info[:raw_address2]]}" if item[table_info[:raw_address2]].size > 0

    zip_match  = item[table_info[:raw_zip]].match(/^\d{4,5}/)
    clean_zip = if zip_match.nil?
                  ''
                else
                  zip_match[0].nil? || zip_match[0].size < 4 ? '' : zip_match[0].rjust(5, '0')
                end

    city_name, city_org_id = get_city_org_id(record_data[table_info[:state]], city_name, route)
    # puts "#{record_data[table_info[:raw_city]]} -- #{city_name} -- #{city_org_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_city]} = '#{escape(city_name)}',
        city_org_id = #{city_org_id.nil? ? "NULL" : "#{city_org_id}"},
        #{table_info[:clean_zip]} ='#{clean_zip}',
        #{table_info[:clean_address]} = '#{escape(MiniLokiC::Formatize::Address.abbreviated_streets(raw_address))}'
      WHERE #{table_info[:raw_city]} = '#{escape(item[table_info[:raw_city]])}'
        AND #{table_info[:state]}='#{escape(item[table_info[:state]])}'
        AND #{table_info[:raw_zip]}='#{escape(item[table_info[:raw_zip]])}'
        AND #{table_info[:raw_address1]}='#{escape(item[table_info[:raw_address1]])}'
        AND #{table_info[:raw_address2]}='#{escape(item[table_info[:raw_address2]])}'
        AND #{table_info[:clean_address]} IS NULL;
    SQL
    # puts query.red
    route.query(query)
  end
end

def get_city_org_id(state_code, city, route)
  query = <<~SQL
    SELECT id, short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    [city, 'NULL']
  else
    [res.first['short_name'], res.first['pl_production_org_id']]
  end
end

def get_state_list(route)
  query = <<~SQL
    SELECT name AS state
    FROM hle_resources_readonly_sync.usa_administrative_division_states;
  SQL
  route.query(query).to_a.map{|i| i['state']}
end

def set_similar_to(table_info, route)
  condition_list = ('a'..'z').to_a.map{|i| "LIKE '#{i}%'"} << "NOT REGEXP '^[a-z]'"
  condition_list.each do |condition|
    query = similar_to_query(table_info, condition)
    puts query.red
    route.query(query)
  end
end

def similar_to_query(table_info, condition)
  <<~SQL
    UPDATE #{table_info[:clean_table]} t1
    LEFT JOIN (SELECT MIN(id) id,
          #{table_info[:clean_name]},
          #{table_info[:name_type]},
          #{table_info[:state]},
          #{table_info[:clean_city]},
          #{table_info[:clean_zip]},
          #{table_info[:clean_address]}
        FROM #{table_info[:clean_table]}
        WHERE #{table_info[:clean_name]} #{condition}
        GROUP BY #{table_info[:clean_name]},
          #{table_info[:name_type]},
          #{table_info[:state]},
          #{table_info[:clean_city]},
          #{table_info[:clean_zip]},
          #{table_info[:clean_address]}) t2
    ON t1.#{table_info[:clean_name]} = t2.#{table_info[:clean_name]}
      AND t1.#{table_info[:name_type]} = t2.#{table_info[:name_type]}
      AND t1.#{table_info[:state]} = t2.#{table_info[:state]}
      AND t1.#{table_info[:clean_city]} = t2.#{table_info[:clean_city]}
      AND t1.#{table_info[:clean_zip]} = t2.#{table_info[:clean_zip]}
      AND t1.#{table_info[:clean_address]} = t2.#{table_info[:clean_address]}
    SET t1.similar_to=t2.id
    WHERE t1.#{table_info[:clean_name]} #{condition}
      AND t1.similar_to IS NULL;
  SQL
end

def set_entity_id(table_info, route)
  query = <<~SQL
    UPDATE #{table_info[:raw_table]} c
      JOIN #{table_info[:clean_table]} cl
        ON c.#{table_info[:raw_name]} = cl.#{table_info[:raw_name]}
          AND c.#{table_info[:state]} = cl.#{table_info[:state]}
          AND c.#{table_info[:raw_city]} = cl.#{table_info[:raw_city]}
          AND c.#{table_info[:raw_zip]} = cl.#{table_info[:raw_zip]}
          AND c.#{table_info[:raw_address1]} = cl.#{table_info[:raw_address1]}
          AND c.#{table_info[:raw_address2]} = cl.#{table_info[:raw_address2]}
    SET c.#{table_info[:entity_id]}=cl.similar_to
      WHERE c.#{table_info[:entity_id]} IS NULL
        AND c.#{table_info[:raw_name]} IS NOT NULL
        AND c.#{table_info[:raw_name]} <> ''
        AND c.active=1;
  SQL
  # puts query.red
  route.query(query)
end

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
