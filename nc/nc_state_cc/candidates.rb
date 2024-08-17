# Creator: Alex Kuzmenko

def cmtes_cands_standard_query
  <<~SQL
    SELECT
      sboeid AS cmte_id,
      candidate_name AS cand_crude,
      searched_name AS cand_key_name
    FROM nc_campaign_candidates2committees
    WHERE status RLIKE 'ACTIVE';
  SQL
end

def cands_list_to_match_query
  <<~SQL
    SELECT
      id,
      name_on_ballot AS cand,
      first_name AS fn,
      middle_name AS mn,
      last_name AS ln
    FROM nc_campaign_finance_candidates2
    WHERE STR_TO_DATE(election_dt, '%d/%m/%Y') = (SELECT MAX(STR_TO_DATE(election_dt, '%d/%m/%Y')) AS dd
                                                  FROM nc_campaign_finance_candidates2)
      AND state = 'NC';
  SQL
end

def candidates_cleaning
  begin
    db13 = C::Mysql.on(DB13, 'nc_raw')

    standard_list = db13.query(cmtes_cands_standard_query).to_a
    list_to_match = db13.query(cands_list_to_match_query).to_a

    list_to_match.each do |cand_row|
      fn = cand_row['fn']
      mn = cand_row['mn']
      ln = cand_row['ln']
      crude_name_to_compare = "#{fn} #{mn} #{ln}"

      possible_raws = standard_list.select { |row| row['cand_crude'].include?(fn) && row['cand_crude'].include?(ln) }

      if possible_raws.empty?
        puts("#{crude_name_to_compare}; #{cand_row['cand']}")
        next
      end

      dl = DamerauLevenshtein
      possible_raws.each do |row|
        row['distance'] = dl.distance(crude_name_to_compare.upcase, row['cand_crude'], 1, 3)
        row['addit_distance'] = [dl.distance(fn.upcase, row['cand_key_name'], 1, 3),
                                 dl.distance(mn.upcase, row['cand_key_name'], 1, 3),
                                 dl.distance(ln.upcase, row['cand_key_name'], 1, 3)].min
      end

      exact_raw = possible_raws.min_by { |row| [row['org'], row['dop_org']] }

      update_query = <<~SQL
        UPDATE nc_campaign_finance_candidates2
        SET cmte_id = #{exact_raw['cmte_id'].dump}
        WHERE id = #{cand_row['id']};
      SQL
      db13.query(update_query)
    end
  rescue Mysql2::Error => e
    p e
  rescue Exception => e
    puts "!!\nException of SQL update algorithm\n!!"
    p e
  ensure
    db13.close if db13
  end
  puts 'Done'
end
